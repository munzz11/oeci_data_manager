#!/usr/bin/env python3

import sys
import pathlib

from PyQt5.QtWidgets import QApplication, QInputDialog, QMainWindow, QTreeWidgetItem, QProgressDialog
from PyQt5 import uic
from PyQt5 import QtCore

from config import ConfigPath

from project import Project
from file_info import FileInfo

from hash_handler import HashHandler
from ros_bag_handler import RosBagHandler
from ros_bag_index_handler import RosBagIndexHandler

class OECIDataManager(QMainWindow):
  handlers = [HashHandler, RosBagIndexHandler, RosBagHandler]

  def __init__(self):
    super().__init__()
    self.config = None
    uic.loadUi(pathlib.Path(__file__).parent/'odm_ui.ui', self)

    self.menuProject.triggered.connect(self.on_project_action)
    self.scanPushButton.clicked.connect(self.on_scan_clicked)
    self.processPushButton.clicked.connect(self.on_process_clicked)
    self.fileTreeWidget.itemSelectionChanged.connect(self.on_file_tree_selection_changed)
    self.fileTreeWidget.setStyleSheet('QTreeWidget#fileTreeWidget::item {background-color: none;}')
    self.progress_dialog = None
    self.need_processing_size = 0

  def set_config(self, config):
    self.config = config
    
  def on_project_action(self, action):
    if action.text() == 'Select Project':
      projects = self.config.get_projects()

      d = QInputDialog(self)
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
    i = self.fileTreeWidget.topLevelItem(0)
    if i is not None:
      project = i.data(0,100)
      QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
      project.scan_source()
      project.scan(OECIDataManager.handlers)
      self.update_stats(project, self.projectStats)
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
    i = self.fileTreeWidget.topLevelItem(0)
    if i is not None:
      project = i.data(0,100)
      QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
      pcount = self.processCountSpinBox.value()
      print(pcount, 'jobs')
      stats = project.generate_file_stats()
      self.need_processing_size = stats['needs_processing']['size']
      self.progress_dialog = QProgressDialog()
      self.progress_dialog.setMaximum(self.need_processing_size)
      self.progress_dialog.show()
      project.process(OECIDataManager.handlers, pcount, self.on_process_progress)
      self.progress_dialog.cancel()
      self.progress_dialog = None
      self.update_stats(project, self.projectStats)
      self.update_files()
      QApplication.restoreOverrideCursor()


  def set_project(self, project: Project):
    QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
    self.fileTreeWidget.clear()
    project.load()
    root = QTreeWidgetItem(self.fileTreeWidget,(project.label,))
    root.setData(0,100,project)
    self.update_stats(project, self.projectStats)
    self.update_files()
    QApplication.restoreOverrideCursor()

  def project(self) -> Project:
    root = self.fileTreeWidget.topLevelItem(0)
    if root is not None:
      project = root.data(0,100)
      if isinstance(project, Project):
        return project
    return None

  def update_files(self):
    root = self.fileTreeWidget.topLevelItem(0)
    if root is not None:
      project = root.data(0,100)

      for f in project():
        current_item = root
        new_item = False
        current_path = None
        for level in f.local_path.parts:
          child = None
          if current_path is None:
            current_path = pathlib.Path(level)
          else:
            current_path = current_path/level
          for c in range(current_item.childCount()):
            child = current_item.child(c)
            if child.text(0) == level:
              break
            else:
              child = None
          if child is None:
            child = QTreeWidgetItem(current_item, (level,))
            child.setData(0,100,current_path)
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

  def update_stats(self, project, widget, path = None):
    stats = project.generate_file_stats(path)
    widget.update_stats(stats)

  def on_file_tree_selection_changed(self):
    i = self.fileTreeWidget.currentItem()
    self.metaTreeWidget.clear()
    if i is None:
      self.selectedDisplayLabel.setText('(none)')
      self.selectedStats.clear_stats()
      return
    d = i.data(0,100)
    if isinstance(d, pathlib.Path):
      self.selectedDisplayLabel.setText(str(d))
      self.update_stats(self.project(), self.selectedStats, d)
    elif isinstance(d, FileInfo):
      self.selectedDisplayLabel.setText(str(d.local_path))
      self.selectedStats.clear_stats()
      if d.meta is not None:
        self.populate_meta_tree(self.metaTreeWidget, d.meta)
    else:
      self.selectedDisplayLabel.setText('(none)')
      self.selectedStats.clear_stats()
    
  def populate_meta_tree(self, parent, item):
    for key in item:
      value = item[key]
      if isinstance(value, list):
        key_item = QTreeWidgetItem(parent,(key,))
        for li in value:
          QTreeWidgetItem(key_item, (str(li),))
      elif isinstance(value, dict):
        self.populate_meta_tree(QTreeWidgetItem(parent, (key,)),value)
      else:
        QTreeWidgetItem(parent, (key+': '+str(value),))

def launch(config):
  app = QApplication(sys.argv)
 
  window = OECIDataManager()
  window.set_config(config)
  window.show()

  sys.exit(app.exec_())
