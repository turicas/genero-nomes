import csv
import io
import lzma
import sqlite3
from multiprocessing import Pool

import requests
from rows.plugins.utils import ipartition, slug
from tqdm import tqdm


def first_name(full_name):
    return slug(full_name).split('_')[0].upper()


def correct_names(names):
    names = set(first_name(name) for name in names)
    return [name for name in names if len(name) >= 3]


def download_name_data(name, sex, timeout=10, max_retries=5):
    if len(name) < 3:
        return None
    api_url = 'http://servicodados.ibge.gov.br/api/v1/censos/nomes/basica?nome={name}&sexo={sex}'
    session = requests.Session()
    session.mount(
        'http://',
        requests.adapters.HTTPAdapter(max_retries=max_retries),
    )
    response = session.get(api_url.format(name=name, sex=sex), timeout=timeout)
    data = response.json()
    if not data or isinstance(data, dict):
        return None

    assert len(data) == 1
    return {
        'name': data[0]['nome'],
        'frequency': data[0]['freq'],
        'alternative_names': data[0]['nomes'].split(','),
    }


def download_name_stats(full_name):
    first_name = slug(full_name).split('_')[0]

    female = download_name_data(first_name, 'f')
    male = download_name_data(first_name, 'm')

    if female is None and male is None:
        return None

    alternative_names = []
    if female is not None:
        alternative_names += female['alternative_names']
    if male is not None:
        alternative_names += male['alternative_names']
    first_name = female['name'] if female is not None else male['name']
    female_frequency = female['frequency'] if female is not None else None
    male_frequency = male['frequency'] if male is not None else None

    if female_frequency and not male_frequency:
        classification = 'F'
        ratio = 1
    elif male_frequency and not female_frequency:
        classification = 'M'
        ratio = 1
    else:
        total = float(female_frequency + male_frequency)
        if female_frequency >= male_frequency:
            classification = 'F'
            ratio = female_frequency / total
        else:
            classification = 'M'
            ratio = male_frequency / total

    return {
        'alternative_names': sorted(set(alternative_names)),
        'classification': classification,
        'frequency_female': female_frequency,
        'frequency_male': male_frequency,
        'ratio': ratio,
    }


class NameGroup:

    def __init__(self):
        self.__frequencies = {}
        self.__frequencies_female = {}
        self.__frequencies_male = {}

    def add(self, name, frequency_female, frequency_male):
        frequency_female = frequency_female or 0
        frequency_male = frequency_male or 0
        frequency = frequency_female + frequency_male
        self.__frequencies[name] = frequency
        self.__frequencies_female[name] = frequency_female
        self.__frequencies_male[name] = frequency_male

    def __contains__(self, name):
        return name in self.__frequencies

    @property
    def frequency_female(self):
        return sum(self.__frequencies_female.values())

    @property
    def frequency_male(self):
        return sum(self.__frequencies_male.values())

    @property
    def classification(self):
        return 'F' if self.frequency_female >= self.frequency_male else 'M'

    @property
    def ratio(self):
        frequency_female = self.frequency_female
        frequency_male = self.frequency_male
        total = float(frequency_female + frequency_male)
        freq = frequency_female if self.classification == 'F' else frequency_male
        return freq / total

    @property
    def name(self):
        return sorted(
            self.__frequencies.items(),
            key=lambda item: item[1],
            reverse=True,
        )[0][0]

    @property
    def names(self):
        return sorted(self.__frequencies.keys())

    @property
    def frequency(self):
        return sum(self.__frequencies.values())


def serialize_row(name, result):
    freq_female = result['frequency_female'] if result else None
    freq_male = result['frequency_male'] if result else None

    if result is None:  # Not found on IBGE Nomes API
        return [
            '',
            '?',
            None,
            None,
            (freq_female or 0) + (freq_male or 0),
            0,
            name,
        ]

    alternative_names = '|'.join(result['alternative_names'])
    return [
        alternative_names,
        result['classification'],
        freq_female,
        freq_male,
        (freq_female or 0) + (freq_male or 0),
        result['ratio'],
        name,
    ]


class NamesByGender:

    def __init__(self, connection, tablename='nomes', group_tablename='grupos',
                 batch_size=100000):
        self.connection = connection
        self.batch_size = batch_size
        self.tablename = tablename
        self.group_tablename = group_tablename

    def _vacuum_db(self):
        cursor = self.connection.cursor()
        cursor.execute('VACUUM')
        connection.commit()

    def create_database(self, input_filename, encoding='utf-8'):
        connection = self.connection
        tablename = self.tablename
        fields = {
            'alternative_names': 'TEXT',
            'classification': 'TEXT',
            'first_name': 'TEXT',
            'frequency_female': 'INT',
            'frequency_male': 'INT',
            'frequency_total': 'INT',
            'frequency_group': 'INT',
            'group_name': 'TEXT',
            'ratio': 'FLOAT',
        }
        temptable = f'{tablename}_temp'
        field_types = ', '.join(f'{name} {type_}'
                                for name, type_ in fields.items())
        sql_drop_table = 'DROP TABLE IF EXISTS {tablename}'
        sql_create_temptable = f'CREATE TABLE {temptable} ({field_types})'
        sql_create_index = '''
            CREATE INDEX idx_{tablename}_name_classification
                ON {tablename} (first_name, classification)
        '''
        sql_create_table = f'''
            CREATE TABLE {tablename} AS
                SELECT * FROM {temptable} GROUP BY first_name ORDER BY first_name
        '''

        cursor = connection.cursor()
        cursor.execute(sql_drop_table.format(tablename=temptable))
        cursor.execute(sql_drop_table.format(tablename=tablename))
        cursor.execute(sql_create_temptable)
        connection.commit()

        fobj = io.TextIOWrapper(
            lzma.open(input_filename, mode='r'),
            encoding=encoding,
        )
        for batch in ipartition(tqdm(csv.DictReader(fobj)), self.batch_size):
            self._insert_names(
                temptable,
                [row['name'] for row in batch
                 if row['document_type'] == 'CPF'],
            )
        cursor.execute(sql_create_index.format(tablename=temptable))
        connection.commit()

        cursor.execute(sql_create_table)
        cursor.execute(sql_create_index.format(tablename=tablename))
        cursor.execute(sql_drop_table.format(tablename=temptable))
        connection.commit()

        self._vacuum_db()

    def count_not_classified(self):
        count_sql = f'''
            SELECT COUNT(*)
            FROM {self.tablename}
            WHERE classification = '' OR classification IS NULL
        '''
        cursor = self.connection.cursor()
        cursor.execute(count_sql)
        return cursor.fetchall()[0][0]

    def classify_names(self, workers=8):
        connection = self.connection
        tablename = self.tablename
        #delete_sql = f'DELETE FROM {tablename} WHERE first_name = ?'
        query = f'''
            SELECT first_name
            FROM {tablename}
            WHERE classification = '' OR classification IS NULL
        '''
        update_sql = f'''
            UPDATE {tablename}
            SET
                alternative_names = ?,
                classification = ?,
                frequency_female = ?,
                frequency_male = ?,
                frequency_total = ?,
                ratio = ?
            WHERE first_name = ?
        '''

        with Pool(processes=workers) as pool, tqdm() as progress:
            cursor = connection.cursor()
            remaining = self.count_not_classified()
            total = 0
            batch_size = workers * 2

            while remaining:
                cursor.execute(query)
                header = [item[0] for item in cursor.description]
                total += remaining
                progress.total = total

                for batch in ipartition(cursor.fetchall(), batch_size):
                    names = [dict(zip(header, row))['first_name'] for row in batch]
                    results = pool.map(download_name_stats, names)
                    update_data = []
                    for name, result in zip(names, results):

                        update_data.append(serialize_row(name, result))
                    cursor.executemany(update_sql, update_data)
                    connection.commit()
                    progress.n += len(batch)
                    progress.update()
                self.extract_alternatives()
                remaining = self.count_not_classified()
                total += remaining

    def _insert_names(self, tablename, names):
        cursor = self.connection.cursor()
        cursor.executemany(
            f'INSERT INTO {tablename} (first_name) VALUES (?)',
            [[name] for name in correct_names(names)],
        )

    def extract_alternatives(self):
        connection = self.connection
        tablename = self.tablename
        query = f'''
            SELECT first_name, alternative_names
            FROM {tablename}
        '''
        cursor = connection.cursor()
        cursor.execute(query)
        header = [item[0] for item in cursor.description]
        data = [dict(zip(header, row)) for row in cursor.fetchall()]
        names, alternatives = set(), set()
        for row in data:
            names.add(row['first_name'])
            if row['alternative_names']:
                alternatives.update(row['alternative_names'].split('|'))
        new_names = correct_names(alternatives - names)
        for batch in ipartition(new_names, self.batch_size):
            self._insert_names(tablename, batch)
        connection.commit()

    def _export_csv(self, query, filename, encoding):
        cursor = self.connection.cursor()
        cursor.execute(query)
        total = cursor.rowcount
        header = [item[0] for item in cursor.description]

        with lzma.open(filename, mode='w') as binary_fobj:
            fobj = io.TextIOWrapper(binary_fobj, encoding=encoding)
            writer = csv.DictWriter(fobj, fieldnames=header)
            writer.writeheader()
            for batch in ipartition(tqdm(cursor.fetchall(), total=total),
                                    self.batch_size):
                writer.writerows([dict(zip(header, row)) for row in batch])

    def export_csv(self, output_names, output_groups, encoding='utf-8'):
        query = f'''
            SELECT *
            FROM {self.tablename}
            WHERE
                classification != ''
                AND classification != '?'
                AND classification IS NOT NULL
        '''
        self._export_csv(query, output_names, encoding)

        query = f'SELECT * FROM {self.group_tablename}'
        self._export_csv(query, output_groups, encoding)

    def define_groups(self):
        cursor = self.connection.cursor()
        cursor.execute(f'SELECT * FROM {self.tablename}')
        header = [item[0] for item in cursor.description]
        data = list(cursor.fetchall())

        groups, group_by_name = [], {}
        for row in tqdm(data):
            row = dict(zip(header, row))
            first_name = row['first_name']
            alternatives = row['alternative_names']
            alternatives = alternatives.split('|') if alternatives else []
            all_names = alternatives + [first_name]

            this_group = None
            for name in all_names:
                if name in group_by_name:
                    this_group = group_by_name[name]
                    break
            if not this_group:
                this_group = NameGroup()
                groups.append(this_group)
            for name in all_names:
                group_by_name[name] = this_group
            this_group.add(
                first_name,
                row['frequency_female'],
                row['frequency_male'],
            )

        update_sql = f'''
            UPDATE {self.tablename}
            SET group_name = ?, frequency_group = ?
            WHERE first_name IN ({{name_list}})
        '''
        group_drop_sql = f'DROP TABLE IF EXISTS {self.group_tablename}'
        group_create_sql = f'''
            CREATE TABLE {self.group_tablename} (
                name TEXT,
                classification TEXT,
                frequency_female INT,
                frequency_male INT,
                frequency_total INT,
                ratio REAL,
                names TEXT
            )
        '''
        group_insert_sql = f'''
            INSERT INTO {self.group_tablename} (
                name,
                classification,
                frequency_female,
                frequency_male,
                frequency_total,
                ratio,
                names
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        '''
        cursor.execute(group_drop_sql)
        cursor.execute(group_create_sql)
        for group in tqdm(groups):
            names = group.names
            placeholders = ', '.join('?' for _ in names)
            cursor.execute(
                update_sql.format(name_list=placeholders),
                (group.name, group.frequency, *names),
            )
            cursor.execute(
                group_insert_sql,
                (group.name, group.classification, group.frequency_female,
                 group.frequency_male, group.frequency, group.ratio,
                 '|' + '|'.join(group.names) + '|'),
            )
        connection.commit()

        self._vacuum_db()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        'command',
        choices=['create-database', 'classify', 'count-not-classified',
                 'extract-alternatives', 'export-csv', 'define-groups']
    )
    args = parser.parse_args()
    dataset_filename = 'data/input/documentos-brasil.csv.xz'
    db_filename = 'data/output/genero-nomes.sqlite'
    output_names_filename = 'data/output/nomes.csv.xz'
    output_groups_filename = 'data/output/grupos.csv.xz'
    connection = sqlite3.Connection(db_filename)
    executor = NamesByGender(connection)

    if args.command == 'create-database':
        executor.create_database(dataset_filename)

    elif args.command == 'classify':
        executor.classify_names()

    elif args.command == 'count-not-classified':
        print(executor.count_not_classified())

    elif args.command == 'extract-alternatives':
        executor.extract_alternatives()

    elif args.command == 'define-groups':
        executor.define_groups()

    elif args.command == 'export-csv':
        executor.export_csv(output_names_filename, output_groups_filename)
