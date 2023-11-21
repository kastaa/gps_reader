import pandas as pd 
import numpy as np 
from pathlib import Path
from typing import List, Union
import datetime
import fitdecode

from gps_reader.parser.BaseParser import BaseParser

class FitParser(BaseParser):
    _base_keys = {"timestamp", "enhanced_altitude", "position_lat", "position_long"}
    
    
    def __init__(self):
        self.column = ["position_lat", "position_long", "enhanced_altitude"]
        self._clear_data()
    
    def _clear_data(self):
        self.timestamp = []
        self.data = []
    
    def from_file(self, file: Union[Path, str]):
        data = []
        itt = 0
        with fitdecode.FitReader(file) as fit:
            for frame in fit:
                if frame.frame_type == fitdecode.FIT_FRAME_HEADER:
                    continue
                elif frame.frame_type == fitdecode.FIT_FRAME_DEFINITION:
                    continue
                elif frame.frame_type == fitdecode.FIT_FRAME_DATA:
                    if frame.name == "record":
                        self._read_frame_data(frame)
                        itt += 1
                elif frame.frame_type == fitdecode.FIT_FRAME_CRC:
                    continue
                else:
                    raise ValueError("frame type is not supported")
        print(itt)
        df_fit = pd.DataFrame(self.data, index=self.timestamp, columns=self.column)
        df_fit.rename(columns={"position_lat": "LAT",
                               "position_long": "LONG",
                               "enhanced_altitude": "ALT"}, inplace=True)
        self._clear_data()
        return df_fit
    
    def _read_frame_data(self, frame):
        local_dict = dict()
        for field in frame.fields:
            try:
                local_dict[field.name] = frame.get_value(field.name)
            except KeyError:
                continue
        if self._base_keys.issubset(set(local_dict.keys())):
            self.timestamp.append(local_dict["timestamp"].timestamp())
            self._format_gps_data(local_dict)
            self.data.append([local_dict[key] for key in self.column])
    
    def _format_gps_data(self, local_dict):
        local_dict["position_lat"] *= ( 180 / 2 ** 31 )
        local_dict["position_long"] *= ( 180 / 2 ** 31 )