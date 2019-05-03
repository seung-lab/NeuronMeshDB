import collections
import numpy as np
import time
import datetime
import os
import sys
import re
import itertools
import logging

from itertools import chain
from multiwrapper import multiprocessing_utils as mu
from pychunkedgraph.backend import cutting, chunkedgraph_comp, flatgraph_utils
from pychunkedgraph.backend.chunkedgraph_utils import compute_indices_pandas, \
    compute_bitmasks, get_google_compatible_time_stamp, \
    get_time_range_filter, get_time_range_and_column_filter, get_max_time, \
    combine_cross_chunk_edge_dicts, get_min_time, partial_row_data_to_column_dict
from pychunkedgraph.backend.utils import serializers, column_keys, row_keys, basetypes
from pychunkedgraph.backend import chunkedgraph_exceptions as cg_exceptions
# from pychunkedgraph.meshing import meshgen

from google.api_core.retry import Retry, if_exception_type
from google.api_core.exceptions import Aborted, DeadlineExceeded, \
    ServiceUnavailable
from google.auth import credentials
from google.cloud import bigtable
from google.cloud.bigtable.row_filters import TimestampRange, \
    TimestampRangeFilter, ColumnRangeFilter, ValueRangeFilter, RowFilterChain, \
    ColumnQualifierRegexFilter, RowFilterUnion, ConditionalRowFilter, \
    PassAllFilter, RowFilter, RowKeyRegexFilter, FamilyNameRegexFilter
from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.column_family import MaxVersionsGCRule

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union, NamedTuple


HOME = os.path.expanduser("~")
# Setting environment wide credential path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
           HOME + "/.cloudvolume/secrets/google-secret.json"


class MeshDB(object):
    def __init__(self,
                 table_id: str,
                 instance_id: str = "neuronmeshdb",
                 project_id: str = "neuromancer-seung-import",
                 is_new: bool = False,
                 credentials: Optional[credentials.Credentials] = None,
                 client: bigtable.Client = None,
                 logger: Optional[logging.Logger] = None) -> None:

        if logger is None:
            self.logger = logging.getLogger(f"{project_id}/{instance_id}/{table_id}")
            self.logger.setLevel(logging.WARNING)
            if not self.logger.handlers:
                sh = logging.StreamHandler(sys.stdout)
                sh.setLevel(logging.WARNING)
                self.logger.addHandler(sh)
        else:
            self.logger = logger

        if client is not None:
            self._client = client
        else:
            self._client = bigtable.Client(project=project_id, admin=True,
                                           credentials=credentials)

        self._instance = self.client.instance(instance_id)
        self._table_id = table_id

        self._table = self.instance.table(self.table_id)

        if is_new:
            self._check_and_create_table()

    @property
    def client(self) -> bigtable.Client:
        return self._client

    @property
    def instance(self) -> bigtable.instance.Instance:
        return self._instance

    @property
    def table(self) -> bigtable.table.Table:
        return self._table

    @property
    def table_id(self) -> str:
        return self._table_id

    @property
    def instance_id(self):
        return self.instance.instance_id

    @property
    def project_id(self):
        return self.client.project

    @property
    def family_id(self) -> str:
        return "0"

    @property
    def family_ids(self):
        return [self.family_id]

    def _check_and_create_table(self) -> None:
        """ Checks if table exists and creates new one if necessary """
        table_ids = [t.table_id for t in self.instance.list_tables()]

        if not self.table_id in table_ids:
            self.table.create()
            f = self.table.column_family(self.family_id)
            f.create()

            self.logger.info(f"Table {self.table_id} created")

    def get_serialized_info(self):
        """ Rerturns dictionary that can be used to load this ChunkedGraph

        :return: dict
        """
        info = {"table_id": self.table_id,
                "instance_id": self.instance_id,
                "project_id": self.project_id}

        try:
            info["credentials"] = self.client.credentials
        except:
            info["credentials"] = self.client._credentials

        return info

    def read_byte_rows(
            self,
            start_key: Optional[bytes] = None,
            end_key: Optional[bytes] = None,
            end_key_inclusive: bool = False,
            row_keys: Optional[Iterable[bytes]] = None,
            columns: Optional[Union[Iterable[column_keys._Column], column_keys._Column]] = None,
            start_time: Optional[datetime.datetime] = None,
            end_time: Optional[datetime.datetime] = None,
            end_time_inclusive: bool = False) -> Dict[bytes, Union[
                Dict[column_keys._Column, List[bigtable.row_data.Cell]],
                List[bigtable.row_data.Cell]
            ]]:
        """Main function for reading a row range or non-contiguous row sets from Bigtable using
        `bytes` keys.

        Keyword Arguments:
            start_key {Optional[bytes]} -- The first row to be read, ignored if `row_keys` is set.
                If None, no lower boundary is used. (default: {None})
            end_key {Optional[bytes]} -- The end of the row range, ignored if `row_keys` is set.
                If None, no upper boundary is used. (default: {None})
            end_key_inclusive {bool} -- Whether or not `end_key` itself should be included in the
                request, ignored if `row_keys` is set or `end_key` is None. (default: {False})
            row_keys {Optional[Iterable[bytes]]} -- An `Iterable` containing possibly
                non-contiguous row keys. Takes precedence over `start_key` and `end_key`.
                (default: {None})
            columns {Optional[Union[Iterable[column_keys._Column], column_keys._Column]]} --
                Optional filtering by columns to speed up the query. If `columns` is a single
                column (not iterable), the column key will be omitted from the result.
                (default: {None})
            start_time {Optional[datetime.datetime]} -- Ignore cells with timestamp before
                `start_time`. If None, no lower bound. (default: {None})
            end_time {Optional[datetime.datetime]} -- Ignore cells with timestamp after `end_time`.
                If None, no upper bound. (default: {None})
            end_time_inclusive {bool} -- Whether or not `end_time` itself should be included in the
                request, ignored if `end_time` is None. (default: {False})

        Returns:
            Dict[bytes, Union[Dict[column_keys._Column, List[bigtable.row_data.Cell]],
                              List[bigtable.row_data.Cell]]] --
                Returns a dictionary of `byte` rows as keys. Their value will be a mapping of
                columns to a List of cells (one cell per timestamp). Each cell has a `value`
                property, which returns the deserialized field, and a `timestamp` property, which
                returns the timestamp as `datetime.datetime` object.
                If only a single `column_keys._Column` was requested, the List of cells will be
                attached to the row dictionary directly (skipping the column dictionary).
        """

        # Create filters: Column and Time
        filter_ = get_time_range_and_column_filter(
            columns=columns,
            start_time=start_time,
            end_time=end_time,
            end_inclusive=end_time_inclusive)

        # Create filters: Rows
        row_set = RowSet()

        if row_keys is not None:
            for row_key in row_keys:
                row_set.add_row_key(row_key)
        elif start_key is not None and end_key is not None:
            row_set.add_row_range_from_keys(
                start_key=start_key,
                start_inclusive=True,
                end_key=end_key,
                end_inclusive=end_key_inclusive)
        else:
            raise cg_exceptions.PreconditionError("Need to either provide a valid set of rows, or"
                                                  " both, a start row and an end row.")

        # Bigtable read with retries
        rows = self._execute_read(row_set=row_set, row_filter=filter_)

        return rows

    def read_byte_row(
            self,
            row_key: bytes,
            columns: Optional[Union[Iterable[column_keys._Column], column_keys._Column]] = None,
            start_time: Optional[datetime.datetime] = None,
            end_time: Optional[datetime.datetime] = None,
            end_time_inclusive: bool = False) -> \
                Union[Dict[column_keys._Column, List[bigtable.row_data.Cell]],
                      List[bigtable.row_data.Cell]]:
        """Convenience function for reading a single row from Bigtable using its `bytes` keys.

        Arguments:
            row_key {bytes} -- The row to be read.

        Keyword Arguments:
            columns {Optional[Union[Iterable[column_keys._Column], column_keys._Column]]} --
                Optional filtering by columns to speed up the query. If `columns` is a single
                column (not iterable), the column key will be omitted from the result.
                (default: {None})
            start_time {Optional[datetime.datetime]} -- Ignore cells with timestamp before
                `start_time`. If None, no lower bound. (default: {None})
            end_time {Optional[datetime.datetime]} -- Ignore cells with timestamp after `end_time`.
                If None, no upper bound. (default: {None})
            end_time_inclusive {bool} -- Whether or not `end_time` itself should be included in the
                request, ignored if `end_time` is None. (default: {False})

        Returns:
            Union[Dict[column_keys._Column, List[bigtable.row_data.Cell]],
                  List[bigtable.row_data.Cell]] --
                Returns a mapping of columns to a List of cells (one cell per timestamp). Each cell
                has a `value` property, which returns the deserialized field, and a `timestamp`
                property, which returns the timestamp as `datetime.datetime` object.
                If only a single `column_keys._Column` was requested, the List of cells is returned
                directly.
        """
        row = self.read_byte_rows(row_keys=[row_key], columns=columns, start_time=start_time,
                                  end_time=end_time, end_time_inclusive=end_time_inclusive)

        return row.get(row_key, {})

    def _partial_row_data_to_column_dict(self, partial_row_data):
        new_column_dict = {}

        for family_id, column_dict in partial_row_data._cells.items():
            for column_key, column_values in column_dict.items():
                column = column_keys.from_key(family_id, column_key)
                new_column_dict[column] = column_values

        return new_column_dict

    def _execute_read_thread(self, row_set_and_filter: Tuple[RowSet, RowFilter]):
        row_set, row_filter = row_set_and_filter
        if not row_set.row_keys and not row_set.row_ranges:
            # Check for everything falsy, because Bigtable considers even empty
            # lists of row_keys as no upper/lower bound!
            return {}

        range_read = self.table.read_rows(row_set=row_set, filter_=row_filter)

        res = {v.row_key.decode(): v.cells["0"][b"data"][0].value for v in range_read}

        return res

    def _execute_read(self, row_set: RowSet, row_filter: RowFilter = None) \
            -> Dict[bytes, Dict[column_keys._Column, bigtable.row_data.PartialRowData]]:
        """ Core function to read rows from Bigtable. Uses standard Bigtable retry logic
        :param row_set: BigTable RowSet
        :param row_filter: BigTable RowFilter
        :return: Dict[bytes, Dict[column_keys._Column, bigtable.row_data.PartialRowData]]
        """

        # FIXME: Bigtable limits the length of the serialized request to 512 KiB. We should
        # calculate this properly (range_read.request.SerializeToString()), but this estimate is
        # good enough for now
        max_row_key_count = 20000
        n_subrequests = max(1, int(np.ceil(len(row_set.row_keys) /
                                           max_row_key_count)))
        n_threads = min(n_subrequests, 2 * mu.n_cpus)

        row_sets = []
        for i in range(n_subrequests):
            r = RowSet()
            r.row_keys = row_set.row_keys[i * max_row_key_count:
                                          (i + 1) * max_row_key_count]
            row_sets.append(r)

        # Don't forget the original RowSet's row_ranges
        row_sets[0].row_ranges = row_set.row_ranges

        responses = mu.multithread_func(self._execute_read_thread,
                                        params=((r, row_filter)
                                                for r in row_sets),
                                        debug=n_threads == 1,
                                        n_threads=n_threads)

        combined_response = {}
        for resp in responses:
            combined_response.update(resp)

        return combined_response

    def mutate_row(self, row_key: bytes,
                   val_dict: dict,
                   time_stamp: Optional[datetime.datetime] = None,
                   family_id=None
                   ) -> bigtable.row.Row:
        """ Mutates a single row

        :param row_key: serialized bigtable row key
        :param val_dict: Dict[column_keys._TypedColumn: bytes]
        :param time_stamp: None or datetime
        :param family_id: None or str
        :return: list
        """
        if family_id is None:
            family_id = self.family_id

        row = self.table.row(row_key)

        for key, value in val_dict.items():

            row.set_cell(column_family_id=family_id,
                         column=key,
                         value=value,
                         timestamp=time_stamp)
        return row

    def bulk_write(self, rows: Sequence[bigtable.row.DirectRow],
                   slow_retry: bool = True,
                   block_size: int = 2000) -> bool:
        """ Writes a list of mutated rows in bulk

        WARNING: If <rows> contains the same row (same row_key) and column
        key two times only the last one is effectively written to the BigTable
        (even when the mutations were applied to different columns)
        --> no versioning!

        :param rows: list
            list of mutated rows
        :param slow_retry: bool
        :param block_size: int
        """
        if slow_retry:
            initial = 5
        else:
            initial = 1

        retry_policy = Retry(
            predicate=if_exception_type((Aborted,
                                         DeadlineExceeded,
                                         ServiceUnavailable)),
            initial=initial,
            maximum=15.0,
            multiplier=2.0,
            deadline=20)

        for i_row in range(0, len(rows), block_size):
            status = self.table.mutate_rows(rows[i_row: i_row + block_size],
                                            retry=retry_policy)

            if not all(status):
                raise Exception(status)

        return True

    def write_cv_data(self, data_list: list):
        """ Writes many key value pairs from dictionary

        :param data_list: list of dicts
        :return: bool
            success indicator
        """

        rows = []
        for data_entry in data_list:
            val_dict = {b"data": data_entry['content']}
            rows.append(self.mutate_row(data_entry["filename"], val_dict,
                                        family_id=self.family_id))

            if len(rows) > 10000:
                self.bulk_write(rows)
                rows = []

        if len(rows) > 0:
            self.bulk_write(rows)

    def write_data(self, data_dict: dict):
        """ Writes many key value pairs from dictionary

        :param data_dict: dicts
        :return: bool
            success indicator
        """

        rows = []
        for key in data_dict:
            val_dict = {b"data": data_dict[key]}
            rows.append(self.mutate_row(key, val_dict,
                                        family_id=self.family_id))

            if len(rows) > 10000:
                self.bulk_write(rows)
                rows = []

        if len(rows) > 0:
            self.bulk_write(rows)