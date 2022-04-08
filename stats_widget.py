#!/usr/bin/env python3

import pathlib

from PyQt5.QtWidgets import QWidget
from PyQt5 import uic

from odm_utils import human_readable_size

class StatsWidget(QWidget):
  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)
    uic.loadUi(pathlib.Path(__file__).parent/'stats_widget.ui', self)
    self.show()

  def update_stats(self, stats):
    self.totalCountLabel.setText(str(stats['total']['count']))
    self.totalSizeLabel.setText(human_readable_size(stats['total']['size']))
    self.needProcessingCountLabel.setText(str(stats['needs_processing']['count']))
    self.needProcessingSizeLabel.setText(human_readable_size(stats['needs_processing']['size']))
    self.newCountLabel.setText(str(stats['new']['count']))
    self.newSizeLabel.setText(human_readable_size(stats['new']['size']))
    self.updatedCountLabel.setText(str(stats['updated']['count']))
    self.updatedSizeLabel.setText(human_readable_size(stats['updated']['size']))
    self.missingCountLabel.setText(str(stats['missing']['count']))

  def clear_stats(self):
    self.totalCountLabel.clear()
    self.totalSizeLabel.clear()
    self.needProcessingCountLabel.clear()
    self.needProcessingSizeLabel.clear()
    self.newCountLabel.clear()
    self.newSizeLabel.clear()
    self.updatedCountLabel.clear()
    self.updatedSizeLabel.clear()
    self.missingCountLabel.clear()
