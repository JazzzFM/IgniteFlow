# -*- coding: utf-8 -*-
from dataclasses import dataclass, field
from typing import List, NoReturn
from PyPDF2 import PdfFileMerger
import os

@dataclass
class PdfMerger:
    tmp_path: str
    out_file: str

    @classmethod
    def __init__(self: object, tmp_path: str, out_file: str) -> NoReturn:
        self.tmp_path = tmp_path
        self.out_file = out_file

        merger = PdfFileMerger()
        for pdf in self.get_list_of_files():
            merger.append(pdf)

        merger.write(self.out_file)
        merger.close()

    @classmethod
    def get_list_of_files(self: object) -> List:
        """
        This function returns a list of strings with the complete path and name of the differents files
        """
        allFiles = []
        for dirpath, _, filenames in os.walk(self.tmp_path):
            for f in filenames:
                allFiles.append(os.path.abspath(os.path.join(self.tmp_path, f)))

        return allFiles
