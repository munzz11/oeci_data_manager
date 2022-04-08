#!/usr/bin/env python3

import sys
import pathlib

from PyQt5.QtWidgets import QApplication, QInputDialog, QTreeWidgetItem, QProgressDialog
from PyQt5.QtGui import QBrush
from PyQt5 import uic
from PyQt5 import QtCore

from config import ConfigPath

from project import Project
from data_manager_utils import human_readable_size

from hash_handler import HashHandler
from ros_bag_handler import RosBagHandler
from ros_bag_index_handler import RosBagIndexHandler

class DataManager:
  handlers = [HashHandler, RosBagIndexHandler, RosBagHandler]

  def __init__(self, config: ConfigPath, path: pathlib.Path):
    self.config = config
    self.ui = uic.loadUi(path/'data_manager_ui.ui')
    self.ui.menuProject.triggered.connect(self.on_project_action)
    self.ui.scanPushButton.clicked.connect(self.on_scan_clicked)
    self.ui.processPushButton.clicked.connect(self.on_process_clicked)
    self.ui.show()
    self.ui.fileTreeWidget.setStyleSheet('QTreeWidget#fileTreeWidget::item {background-color: none;}')
    self.progress_dialog = None
    self.need_processing_size = 0
    

  def on_project_action(self, action):
    if action.text() == 'Select Project':
      projects = self.config.get_projects()

      d = QInputDialog(self.ui)
      p_list = []
      for p in projects:
        p_list.append(p.label)

      d.setComboBoxItems(p_list)
      d.setModal(True)
      if d.exec():
        for p in projects:
          if p.label == d.textValue():
            self.set_project(p)

  def on_scan_clicked(self):
    i = self.ui.fileTreeWidget.topLevelItem(0)
    if i is not None:
      project = i.data(0,100)
      QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
      project.scan_source()
      project.scan(DataManager.handlers)
      self.update_stats(project)
      self.update_files()
      QApplication.restoreOverrideCursor()

  def on_process_progress(self, processed_size):
    if self.progress_dialog is not None:
      self.progress_dialog.setValue(processed_size)
      if self.progress_dialog.wasCanceled():
        return True
      QApplication.processEvents()
    return False

  def on_process_clicked(self):
    i = self.ui.fileTreeWidget.topLevelItem(0)
    if i is not None:
      project = i.data(0,100)
      QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
      pcount = self.ui.processCountSpinBox.value()
      print(pcount, 'jobs')
      stats = project.generate_file_stats()
      self.need_processing_size = stats['needs_processing']['size']
      self.progress_dialog = QProgressDialog()
      self.progress_dialog.setMaximum(self.need_processing_size)
      self.progress_dialog.show()
      project.process(DataManager.handlers, pcount, self.on_process_progress)
      self.progress_dialog.cancel()
      self.progress_dialog = None
      self.update_stats(project)
      self.update_files()
      QApplication.restoreOverrideCursor()


  def set_project(self, project: Project):
    QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
    self.ui.fileTreeWidget.clear()
    project.load()
    root = QTreeWidgetItem(self.ui.fileTreeWidget,(project.label,))
    root.setData(0,100,project)
    self.update_stats(project)
    self.update_files()
    QApplication.restoreOverrideCursor()


  def update_files(self):
    root = self.ui.fileTreeWidget.topLevelItem(0)
    if root is not None:
      project = root.data(0,100)

      for f in project():
        current_item = root
        new_item = False
        for level in f.local_path.parts:
          child = None
          for c in range(current_item.childCount()):
            child = current_item.child(c)
            if child.text(0) == level:
              break
            else:
              child = None
          if child is None:
            child = QTreeWidgetItem(current_item, (level,))
            current_item.sortChildren(0, QtCore.Qt.AscendingOrder)
            new_item = True
          current_item = child
        if new_item:
          current_item.setData(0,100,f)
        s = f.status()
        if s == 'up-to-date':
          current_item.setBackground(0, QtCore.Qt.green)
        elif s == 'modified' or s == 'needs processing':
          current_item.setBackground(0, QtCore.Qt.yellow)
        elif s == 'missing':
          current_item.setBackground(0, QtCore.Qt.red)
        else:
          current_item.setBackground(0, QtCore.Qt.lightGray)

  def update_stats(self, project):
    stats = project.generate_file_stats()
    self.ui.totalCountLabel.setText(str(stats['total']['count']))
    self.ui.totalSizeLabel.setText(human_readable_size(stats['total']['size']))
    self.ui.needProcessingCountLabel.setText(str(stats['needs_processing']['count']))
    self.ui.needProcessingSizeLabel.setText(human_readable_size(stats['needs_processing']['size']))
    self.ui.newCountLabel.setText(str(stats['new']['count']))
    self.ui.newSizeLabel.setText(human_readable_size(stats['new']['size']))
    self.ui.updatedCountLabel.setText(str(stats['updated']['count']))
    self.ui.updatedSizeLabel.setText(human_readable_size(stats['updated']['size']))
    self.ui.missingCountLabel.setText(str(stats['missing']['count']))

def launch(config):
  app = QApplication(sys.argv)
  
  window = DataManager(config, pathlib.Path(__file__).parent)
  sys.exit(app.exec_())
