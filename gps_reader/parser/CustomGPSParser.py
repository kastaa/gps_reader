import pandas as pd 
import numpy as np 
from pathlib import Path
from typing import List, Union
import datetime

from gps_reader.parser.BaseParser import BaseParser


COLUMN_IMU_INDEX = 0
COLUMN_GPS_INDEX = 1
COLUMN_BMP_INDEX = 2
DATA_INDEX = 3

INDEX_MILLISECOND = 1
INDEX_GPS_DATA = (2, 5)
INDEX_GPS_TIME = (6, 8)
INDEX_IMU_DATA = (2, 4)
INDEX_BMP_DATA = (2, 5)

DEFAULT_DATE = (2023, 11, 18)

class CustomGPSParser(BaseParser):
    
    def __init__(self):
        self._parse_function = {"gps": self._parse_gps_line, 
                                "imu": self._parse_imu_line,
                                "bmp": self._parse_bmp_line}
        self._clear_data()
        
    def _clear_data(self):
        self.timestamp_gps = []
        self.timestamp_imu = []
        self.timestamp_bmp = []
        self._millisec_gps = []
        self._millisec_imu = []
        self._millisec_bmp = []
        self.gps_data = []
        self.imu_data = []
        self.bmp_data = []
        self.column_gps = []
        self.column_imu = []
        self.column_bmp = []
        
    def _parse_gps_line(self, split_line: List[str]):
        self._millisec_gps.append(int(split_line[INDEX_MILLISECOND]))
        self.gps_data.append([float(v) for v in split_line[INDEX_GPS_DATA[0]:INDEX_GPS_DATA[1] + 1]])
        date = [int(v) for v in self.date_acquisition]
        time = [int(v) for v in split_line[INDEX_GPS_TIME[0]:INDEX_GPS_TIME[1] + 1]]
        timestamp_noformat = datetime.datetime(*date, *time)
        self.timestamp_gps.append(timestamp_noformat.timestamp() - 14400)
    
    def _parse_imu_line(self, split_line: List[str]):
        self._millisec_imu.append(int(split_line[INDEX_MILLISECOND]))
        self.imu_data.append([float(v) for v in split_line[INDEX_IMU_DATA[0]:INDEX_IMU_DATA[1] + 1]])
        
    def _parse_bmp_line(self, split_line: List[str]):
        self._millisec_bmp.append(int(split_line[INDEX_MILLISECOND]))
        self.timestamp_bmp.append(self.timestamp_gps[-1])
        self.bmp_data.append([float(v) for v in split_line[INDEX_BMP_DATA[0]:INDEX_BMP_DATA[1] + 1]])
        
    def _parse_line(self, line: str):
        split_line = line.split(';')
        sensor_key = split_line[0]
        try:
            self._parse_function[sensor_key](split_line)
        except KeyError:
            raise ValueError("Unrecognized line in file")
                         
    def from_file(self, file: Union[Path, str], date_acquisition=None):
        with open(file, "rt") as txtfile:
            full_lines = txtfile.readlines()
        full_lines = [line.rstrip("\n") for line in full_lines]
        self.column_gps = full_lines[COLUMN_GPS_INDEX].split(";")[INDEX_GPS_DATA[0]:INDEX_GPS_DATA[1] + 1]
        self.column_imu = full_lines[COLUMN_IMU_INDEX].split(";")[INDEX_IMU_DATA[0]:INDEX_IMU_DATA[1] + 1]
        self.column_bmp = full_lines[COLUMN_BMP_INDEX].split(";")[INDEX_BMP_DATA[0]:INDEX_BMP_DATA[1] + 1]
        
        if date_acquisition is None:
            self.date_acquisition = DEFAULT_DATE
        else:
            self.date_acquisition = date_acquisition
    
        for line in full_lines[DATA_INDEX:]:
            self._parse_line(line)

        gps_df = pd.DataFrame(self.gps_data, index=self.timestamp_gps, columns=self.column_gps)
        bmp_df = pd.DataFrame(self.bmp_data, index=self.timestamp_bmp, columns=self.column_bmp)
        self._clear_data()
        return gps_df, bmp_df