#Note: Only rooftop PV in the DATASETS_Archive dict is updated. Fix before using others

from io import BytesIO
import datetime
import re
from collections import namedtuple
import requests
from nemweb import nemfile_reader, nemweb_sqlite_oo


class ArchiveFileHandler:
    """class for handling 'ARCHIVE' nemweb files from http://www.nemweb.com.au
    Requires a 'ArchiveDataset' namedtuple with following fields:

    - nemweb_name: the name of the dataset to be download (e.g. Dispatch_SCADA)
    - zipname_pattern: a regex expression to match subfolder in the zipfiles
    - filename_pattern: a regex expression to match and a determine datetime from filename
      on nemweb. As example, for files in the Dispatch_SCADA dataset
      (e.g "PUBLIC_DISPATCHSCADA_201806201135_0000000296175732.zip") the regex
      file_patten is PUBLIC_DISPATCHSCADA_([0-9]{12})_[0-9]{16}.zip
    - the format of the string to strip the datetime from. From the above example, the
      match returns '201806201135', so the string is "%Y%m%d%H%M",
    - the list of tables to insert from each dataset. This is derived from the 2nd and
      3rd column in the nemweb dataset. For example, the 2nd column is in Dispatch_SCADA
      is "DISPATCH" and the 3rd is "SCADA_VALUE" and the name is "DISPATCH_UNIT_SCADA".

    Several datasets contain multiple tables. Examples can be found in the DATASETS dict
    (nemweb_reader.DATASETS)"""

    def __init__(self, db_name='nemweb_live.db'):
        self.base_url = "http://www.nemweb.com.au"
        self.section = "Reports/ARCHIVE"
        self.db = nemweb_sqlite_oo.DBHandler(db_name)

    def determine_start_date(self, start_date=None):

        if start_date:
            start_date = datetime.datetime.strptime(start_date, '%Y%m%d')
        else:
            try:
                start_date= self.db.get_table_latest_record(dataset.tales[0],
                                                            dataset.timestamp_col)
            except:
                raise ValueError("Couldn't retrieve start_date automatically, please enter manually")
        return start_date

    def update_data(
            self,
            dataset,
            print_progress=False,
            start_date=None,
            end_date='30001225',  #  must be a better way
    ):
        """Main method to process nemweb dataset
        - downloads the index page for the dataset
        - determines date to start downloading from
        - matches the start date against files in the index
        - inserts new files into database"""
        assert (self.db is not None), 'Create a DBHandler first!'
        self.dataset = dataset

        for table in dataset.tables:
            if not self.db.check_table_existence(table):
                self.db.create_table(table, dataset.keycols, dataset.colnames)
                print(f'Created table {table}')


        start_date = self.determine_start_date(start_date)
        end_date = datetime.datetime.strptime(end_date, '%Y%m%d')

        page = requests.get("{0}/{1}/{2}/".format(self.base_url,
                                                  self.section,
                                                  dataset.dataset_name))
        regex_parent_dir = re.compile("/{0}/{1}/{2}".format(self.section,
                                                 dataset.dataset_name,
                                                 dataset.zipfile_pattern))

        for match in regex_parent_dir.finditer(page.text):
            parent_start_datetime = datetime.datetime.strptime(match.group(1), dataset.zipfile_datetime_format)
            if parent_start_datetime < (start_date-datetime.timedelta(dataset.zipfile_day_delay)):
            #archive parent files are weekly or monthly, so we build in some flexibility when choosing to download
                continue

            nemfile_list = self.download(match.group(0))

            for nemfile in nemfile_list:
                date_regex = re.compile(dataset.nemfile_pattern)
                timestring = date_regex.search(nemfile[0]).group(1)
                file_datetime = datetime.datetime.strptime(timestring, dataset.datetime_format)
                if end_date > file_datetime >= start_date:
                    if print_progress:
                        print(dataset.dataset_name, file_datetime)
                    colnames = [k for k, _ in dataset.colnames.items()]
                    for table in dataset.tables:
                        dataframe = nemfile[1][table].drop_duplicates().copy()
                        self.db.insert(dataframe[colnames], table)

    def download(self, link):
        """Dowloads nemweb zipfile from link into memory as a byteIO object.
        nemfile object is returned from the byteIO object """
        response = requests.get("{0}{1}".format(self.base_url, link))
        zip_bytes = BytesIO(response.content)
        nemfile_list = nemfile_reader.nemzip_reader_archive(zip_bytes,self.dataset.idcols)
        return nemfile_list


#  class factory function for containing data for 'Archive' datasets
ArchiveDataset = namedtuple("NemwebArchiveFile",
                            ["dataset_name",
                             "zipfile_pattern",
                             "nemfile_pattern",
                             "zipfile_day_delay",
                             "zipfile_datetime_format",
                             "datetime_format",
                             "datetime_column",
                             "tables",
                             "idcols",
                             "keycols",
                             "colnames"])

DATASETS_ARCHIVE = {"rooftopPV_actual": ArchiveDataset(
    dataset_name="ROOFTOP_PV/ACTUAL",
    zipfile_pattern="PUBLIC_ROOFTOP_PV_ACTUAL_([0-9]{8}).zip",
    nemfile_pattern="PUBLIC_ROOFTOP_PV_ACTUAL_([0-9]{14})_[0-9]{16}.zip",
    zipfile_day_delay=10,
    zipfile_datetime_format="%Y%m%d",
    datetime_format="%Y%m%d%H%M00",
    datetime_column="INTERVAL_DATETIME",
    tables=['ROOFTOP_ACTUAL'],
    idcols=[1, 2],
    keycols=['INTERVAL_DATETIME', 'REGIONID'],
    colnames={'INTERVAL_DATETIME':'text', 'REGIONID':'text', 'POWER':'real', 'QI':'integer'}),
"operational_demand": ArchiveDataset(
    dataset_name="Operational_Demand/ACTUAL_HH",
    zipfile_pattern="PUBLIC_ACTUAL_OPERATIONAL_DEMAND_HH_([0-9]{8}).zip",
    nemfile_pattern="PUBLIC_ACTUAL_OPERATIONAL_DEMAND_HH_([0-9]{12})_[0-9]{14}.zip",
    zipfile_day_delay=10,
    zipfile_datetime_format="%Y%m%d",
    datetime_format="%Y%m%d%H%M",
    datetime_column="INTERVAL_DATETIME",
    tables=['OPERATIONAL_DEMAND_ACTUAL'],
    idcols=[1, 2],
    keycols=['INTERVAL_DATETIME', 'REGIONID'],
    colnames={'INTERVAL_DATETIME':'text', 'REGIONID':'text', 'OPERATIONAL_DEMAND':'real'}),
"price_hh": ArchiveDataset(
    dataset_name="Public_Prices",
    zipfile_pattern="PUBLIC_PRICES_([0-9]{8}).zip",
    nemfile_pattern="PUBLIC_PRICES_([0-9]{12})_[0-9]{14}.zip",
    zipfile_day_delay=35,
    zipfile_datetime_format="%Y%m%d",
    datetime_format="%Y%m%d%H%M",
    datetime_column="SETTLEMENTDATE",
    tables=['TREGION_2'],
    idcols=[1, 3],
    keycols=['SETTLEMENTDATE', 'REGIONID'],
    colnames={'SETTLEMENTDATE':'text', 'REGIONID':'text', 'RRP':'real',
              'INVALIDFLAG': 'real', 'RAISE6SECRRP':'real', 'RAISE60SECRRP':'real',
              'RAISE5MINRRP':'real', 'RAISEREGRRP':'real', 'LOWER6SECRRP':'real',
              'LOWER60SECRRP':'real', 'LOWER5MINRRP':'real', 'LOWERREGRRP':'real'}
)}





def update_datasets_archive(datasets, print_progress=False, start_date=None):
    """function that updates a subset of datasets (as a list) contained in DATASETS_ARCHIVE"""
    filehandler = ArchiveFileHandler('nemweb_live.db')
    for dataset_name in datasets:
        filehandler.update_data(DATASETS_ARCHIVE[dataset_name],
                                print_progress=print_progress,
                                start_date=start_date)
