"""
SQL-style merge routines
"""

import copy
import datetime
from functools import partial
import string
from typing import TYPE_CHECKING, Optional, Tuple, Union
import warnings
import timeit
import numpy as np

from pandas._libs import Timedelta, hashtable as libhashtable, lib
import pandas._libs.join as libjoin
from pandas.errors import MergeError
from pandas.util._decorators import Appender, Substitution

from pandas.core.dtypes.common import (
    ensure_float64,
    ensure_int64,
    ensure_object,
    is_array_like,
    is_bool,
    is_bool_dtype,
    is_categorical_dtype,
    is_datetime64tz_dtype,
    is_dtype_equal,
    is_extension_array_dtype,
    is_float_dtype,
    is_integer,
    is_integer_dtype,
    is_list_like,
    is_number,
    is_numeric_dtype,
    is_object_dtype,
    needs_i8_conversion,
)
from pandas.core.dtypes.generic import ABCDataFrame, ABCSeries
from pandas.core.dtypes.missing import isna, na_value_for_dtype

from pandas import Categorical, Index, MultiIndex
from pandas._typing import FrameOrSeries
import pandas.core.algorithms as algos
from pandas.core.arrays.categorical import _recode_for_categories
import pandas.core.common as com
from pandas.core.frame import _merge_doc
from pandas.core.internals import _transform_index, concatenate_block_managers
from pandas.core.sorting import is_int64_overflow_possible

if TYPE_CHECKING:
    from pandas import DataFrame, Series  # noqa:F401


@Substitution("\nleft : DataFrame")
@Appender(_merge_doc, indents=0)
def pipeline_merge(
        left,
        right,
        how: str = "inner",
        on=None,
        left_on=None,
        right_on=None,
        left_index: bool = False,
        right_index: bool = False,
        sort: bool = False,
        suffixes=("_x", "_y"),
        copy: bool = True,
        indicator: bool = False,
        validate=None,
        factorizer=None,
        intfactorizer=None,
        leftsorter=None,
        leftcount=None,
):
    op = _PipelineMergeOperation(
        left,
        right,
        how=how,
        on=on,
        left_on=left_on,
        right_on=right_on,
        left_index=left_index,
        right_index=right_index,
        sort=sort,
        suffixes=suffixes,
        copy=copy,
        indicator=indicator,
        validate=validate,
        factorizer=factorizer,
        intfactorizer=intfactorizer,
        leftsorter=leftsorter,
        leftcount=leftcount,
    )
    return op.get_result()


if __debug__:
    pipeline_merge.__doc__ = _merge_doc % "\nleft : DataFrame"


# TODO: transformations??
# TODO: only copy DataFrames when modification necessary
class _PipelineMergeOperation:
    """
    Perform a database (SQL) merge operation between two DataFrame or Series
    objects using either columns as keys or their row indexes
    """

    _merge_type = "merge"

    def __init__(
            self,
            left: Union["Series", "DataFrame"],
            right: Union["Series", "DataFrame"],
            how: str = "inner",
            on=None,
            left_on=None,
            right_on=None,
            axis=1,
            left_index: bool = False,
            right_index: bool = False,
            sort: bool = True,
            suffixes=("_x", "_y"),
            copy: bool = True,
            indicator: bool = False,
            validate=None,
            factorizer=None,
            intfactorizer=None,
            leftsorter=None,
            leftcount=None,
    ):
        #print("calling this init function")
        _left = _validate_operand(left)
        _right = _validate_operand(right)
        self.left = self.orig_left = _left
        self.right = self.orig_right = _right
        self.how = how
        self.axis = axis

        self.on = com.maybe_make_list(on)
        self.left_on = com.maybe_make_list(left_on)
        self.right_on = com.maybe_make_list(right_on)

        self.copy = copy
        self.suffixes = suffixes
        self.sort = sort
        self.left_sorter = leftsorter
        self.left_count = leftcount

        self.left_index = left_index
        self.right_index = right_index

        self.indicator = indicator

        self.indicator_name: Optional[str]
        if isinstance(self.indicator, str):
            self.indicator_name = self.indicator
        elif isinstance(self.indicator, bool):
            self.indicator_name = "_merge" if self.indicator else None
        else:
            raise ValueError(
                "indicator option can only accept boolean or string arguments"
            )

        if not is_bool(left_index):
            raise ValueError(
                "left_index parameter must be of type bool, not "
                "{left_index}".format(left_index=type(left_index))
            )
        if not is_bool(right_index):
            raise ValueError(
                "right_index parameter must be of type bool, not "
                "{right_index}".format(right_index=type(right_index))
            )

        # warn user when merging between different levels
        if _left.columns.nlevels != _right.columns.nlevels:
            msg = (
                "merging between different levels can give an unintended "
                "result ({left} levels on the left, {right} on the right)"
            ).format(left=_left.columns.nlevels, right=_right.columns.nlevels)
            warnings.warn(msg, UserWarning)
        # TODO this changes the on column to not None
        self._validate_specification()

        # note this function has side effects
        # FIXME 0 get merge keys -> no fix here
        # Left join keys and right join keys all default if not set on
        # Join names should be an empty array
        (
            self.left_join_keys,
            self.right_join_keys,
            self.join_names,
        ) = self._get_merge_keys()
        self.slices = 8
        if factorizer is None:
            print("Size set to be: 1")
            print(max(len(self.left_join_keys[0]), self.slices * len(self.right_join_keys[0])))
            #self.factorizer = libhashtable.Factorizer(max(len(self.left_join_keys[0]), self.slices * len(self.right_join_keys[0])))
            self.factorizer = libhashtable.Factorizer(len(self.left_join_keys[0]) + self.slices * len(self.right_join_keys[0]))
        else:
            print("Size set to be: 2")
            print(max(len(self.left_join_keys[0]), self.slices * len(self.right_join_keys[0])))
            self.factorizer = factorizer

        if intfactorizer is None:
            #self.intfactorizer = libhashtable.Int64Factorizer(max(len(self.left_join_keys[0]), self.slices * len(self.right_join_keys[0])))
            self.intfactorizer = libhashtable.Int64Factorizer(len(self.left_join_keys[0]) + self.slices * len(self.right_join_keys[0]))
        else:
            self.intfactorizer = intfactorizer
        # validate the merge keys dtypes. We may need to coerce
        # to avoid incompat dtypes
        self._maybe_coerce_merge_keys()

        # If argument passed to validate,
        # check if columns specified as unique
        # are in fact unique.
        if validate is not None:
            self._validate(validate)

    def get_result(self):
        # TODO ignore indicators for now
        if self.indicator:
            self.left, self.right = self._indicator_pre_merge(self.left, self.right)
        # FIXME 1 modify indexer
        #print("Check point 11")
        join_index, left_indexer, right_indexer = self._get_join_info()

        ldata, rdata = self.left._data, self.right._data
        lsuf, rsuf = self.suffixes

        llabels, rlabels = _items_overlap_with_suffix(
            ldata.items, lsuf, rdata.items, rsuf
        )

        lindexers = {1: left_indexer} if left_indexer is not None else {}
        rindexers = {1: right_indexer} if right_indexer is not None else {}

        #print("&&&&&&")
        #print(lindexers)
        #print(rindexers)
        #print("Check point 12")
        result_data = concatenate_block_managers(
            [(ldata, lindexers), (rdata, rindexers)],
            axes=[llabels.append(rlabels), join_index],
            concat_axis=0,
            copy=self.copy,
        )
        #print(result_data)

        #print("Check point 13")
        typ = self.left._constructor
        result = typ(result_data).__finalize__(self, method=self._merge_type)

        #print(result)

        if self.indicator:
            result = self._indicator_post_merge(result)

        self._maybe_add_join_keys(result, left_indexer, right_indexer)

        self._maybe_restore_index_levels(result)

        #print("**********")
        #print(result)
       # print("Check point 14")
        #print("######")
        #print(self.left_sorter)
        #print(self.left_count)
        #print("######")
        return result, self.factorizer, self.intfactorizer, self.left_sorter, self.left_count

    def _indicator_pre_merge(
            self, left: "DataFrame", right: "DataFrame"
    ) -> Tuple["DataFrame", "DataFrame"]:

        columns = left.columns.union(right.columns)

        for i in ["_left_indicator", "_right_indicator"]:
            if i in columns:
                raise ValueError(
                    "Cannot use `indicator=True` option when "
                    "data contains a column named {name}".format(name=i)
                )
        if self.indicator_name in columns:
            raise ValueError(
                "Cannot use name of an existing column for indicator column"
            )

        left = left.copy()
        right = right.copy()

        left["_left_indicator"] = 1
        left["_left_indicator"] = left["_left_indicator"].astype("int8")

        right["_right_indicator"] = 2
        right["_right_indicator"] = right["_right_indicator"].astype("int8")

        return left, right

    def _indicator_post_merge(self, result):

        result["_left_indicator"] = result["_left_indicator"].fillna(0)
        result["_right_indicator"] = result["_right_indicator"].fillna(0)

        result[self.indicator_name] = Categorical(
            (result["_left_indicator"] + result["_right_indicator"]),
            categories=[1, 2, 3],
        )
        result[self.indicator_name] = result[self.indicator_name].cat.rename_categories(
            ["left_only", "right_only", "both"]
        )

        result = result.drop(labels=["_left_indicator", "_right_indicator"], axis=1)
        return result

    def _maybe_restore_index_levels(self, result):
        """
        Restore index levels specified as `on` parameters

        Here we check for cases where `self.left_on` and `self.right_on` pairs
        each reference an index level in their respective DataFrames. The
        joined columns corresponding to these pairs are then restored to the
        index of `result`.

        **Note:** This method has side effects. It modifies `result` in-place

        Parameters
        ----------
        result: DataFrame
            merge result

        Returns
        -------
        None
        """
        names_to_restore = []
        for name, left_key, right_key in zip(
                self.join_names, self.left_on, self.right_on
        ):
            if (
                    self.orig_left._is_level_reference(left_key)
                    and self.orig_right._is_level_reference(right_key)
                    and name not in result.index.names
            ):
                names_to_restore.append(name)

        if names_to_restore:
            result.set_index(names_to_restore, inplace=True)

    def _maybe_add_join_keys(self, result, left_indexer, right_indexer):

        left_has_missing = None
        right_has_missing = None

        keys = zip(self.join_names, self.left_on, self.right_on)
        for i, (name, lname, rname) in enumerate(keys):
            if not _should_fill(lname, rname):
                continue

            take_left, take_right = None, None

            if name in result:

                if left_indexer is not None and right_indexer is not None:
                    if name in self.left:

                        if left_has_missing is None:
                            left_has_missing = (left_indexer == -1).any()

                        if left_has_missing:
                            take_right = self.right_join_keys[i]

                            if not is_dtype_equal(
                                    result[name].dtype, self.left[name].dtype
                            ):
                                take_left = self.left[name]._values

                    elif name in self.right:

                        if right_has_missing is None:
                            right_has_missing = (right_indexer == -1).any()

                        if right_has_missing:
                            take_left = self.left_join_keys[i]

                            if not is_dtype_equal(
                                    result[name].dtype, self.right[name].dtype
                            ):
                                take_right = self.right[name]._values

            elif left_indexer is not None and is_array_like(self.left_join_keys[i]):
                take_left = self.left_join_keys[i]
                take_right = self.right_join_keys[i]

            if take_left is not None or take_right is not None:

                if take_left is None:
                    lvals = result[name]._values
                else:
                    lfill = na_value_for_dtype(take_left.dtype)
                    lvals = algos.take_1d(take_left, left_indexer, fill_value=lfill)

                if take_right is None:
                    rvals = result[name]._values
                else:
                    rfill = na_value_for_dtype(take_right.dtype)
                    rvals = algos.take_1d(take_right, right_indexer, fill_value=rfill)

                # if we have an all missing left_indexer
                # make sure to just use the right values
                mask = left_indexer == -1
                if mask.all():
                    key_col = rvals
                else:
                    key_col = Index(lvals).where(~mask, rvals)

                if result._is_label_reference(name):
                    result[name] = key_col
                elif result._is_level_reference(name):
                    if isinstance(result.index, MultiIndex):
                        key_col.name = name
                        idx_list = [
                            result.index.get_level_values(level_name)
                            if level_name != name
                            else key_col
                            for level_name in result.index.names
                        ]

                        result.set_index(idx_list, inplace=True)
                    else:
                        result.index = Index(key_col, name=name)
                else:
                    result.insert(i, name or "key_{i}".format(i=i), key_col)

    def _get_join_indexers(self):
        """ return the join indexers """
        # FIXME 4 fix this function
        return _get_join_indexers(
            self.left_join_keys, self.right_join_keys, self.factorizer, self.intfactorizer, self.left_sorter, self.left_count, sort=self.sort, how=self.how
        )

    def _get_join_info(self):
        left_ax = self.left._data.axes[self.axis]
        right_ax = self.right._data.axes[self.axis]

        if self.left_index and self.right_index and self.how != "asof":
            join_index, left_indexer, right_indexer = left_ax.join(
                right_ax, how=self.how, return_indexers=True, sort=self.sort
            )
        elif self.right_index and self.how == "left":
            join_index, left_indexer, right_indexer = _left_join_on_index(
                left_ax, right_ax, self.left_join_keys, sort=self.sort
            )

        elif self.left_index and self.how == "right":
            join_index, right_indexer, left_indexer = _left_join_on_index(
                right_ax, left_ax, self.right_join_keys, sort=self.sort
            )
        else:
            # FIXME 2 fix get join indexers
            (left_indexer, right_indexer, left_sorter, left_count) = self._get_join_indexers()
            #print("!!!")
            #print(left_sorter)
            #print(left_count)
            #print("!!!")
            if self.left_count is None:
                self.left_count = left_count
            if self.left_sorter is None:
                self.left_sorter = left_sorter
            #print("@@@")
            #print(self.left_sorter)
            #print(self.left_count)
            if self.right_index:
                if len(self.left) > 0:
                    join_index = self._create_join_index(
                        self.left.index,
                        self.right.index,
                        left_indexer,
                        right_indexer,
                        how="right",
                    )
                else:
                    join_index = self.right.index.take(right_indexer)
                    left_indexer = np.array([-1] * len(join_index))
            elif self.left_index:
                if len(self.right) > 0:
                    join_index = self._create_join_index(
                        self.right.index,
                        self.left.index,
                        right_indexer,
                        left_indexer,
                        how="left",
                    )
                else:
                    join_index = self.left.index.take(left_indexer)
                    right_indexer = np.array([-1] * len(join_index))
            else:
                # FIXME 3 fix join index generation
                join_index = Index(np.arange(len(left_indexer)))

        if len(join_index) == 0:
            join_index = join_index.astype(object)
        return join_index, left_indexer, right_indexer

    def _create_join_index(
            self,
            index: Index,
            other_index: Index,
            indexer,
            other_indexer,
            how: str = "left",
    ):
        """
        Create a join index by rearranging one index to match another

        Parameters
        ----------
        index: Index being rearranged
        other_index: Index used to supply values not found in index
        indexer: how to rearrange index
        how: replacement is only necessary if indexer based on other_index

        Returns
        -------
        join_index
        """
        if self.how in (how, "outer") and not isinstance(other_index, MultiIndex):
            # if final index requires values in other_index but not target
            # index, indexer may hold missing (-1) values, causing Index.take
            # to take the final value in target index. So, we set the last
            # element to be the desired fill value. We do not use allow_fill
            # and fill_value because it throws a ValueError on integer indices
            mask = indexer == -1
            if np.any(mask):
                fill_value = na_value_for_dtype(index.dtype, compat=False)
                index = index.append(Index([fill_value]))
        return index.take(indexer)

    def _get_merge_keys(self):
        """
        Note: has side effects (copy/delete key columns)

        Parameters
        ----------
        left
        right
        on

        Returns
        -------
        left_keys, right_keys
        """
        left_keys = []
        right_keys = []
        join_names = []
        right_drop = []
        left_drop = []

        left, right = self.left, self.right

        is_lkey = lambda x: is_array_like(x) and len(x) == len(left)
        is_rkey = lambda x: is_array_like(x) and len(x) == len(right)

        # Note that pd.merge_asof() has separate 'on' and 'by' parameters. A
        # user could, for example, request 'left_index' and 'left_by'. In a
        # regular pd.merge(), users cannot specify both 'left_index' and
        # 'left_on'. (Instead, users have a MultiIndex). That means the
        # self.left_on in this function is always empty in a pd.merge(), but
        # a pd.merge_asof(left_index=True, left_by=...) will result in a
        # self.left_on array with a None in the middle of it. This requires
        # a work-around as designated in the code below.
        # See _validate_specification() for where this happens.

        # ugh, spaghetti re #733
        # TODO goes in the first if
        if _any(self.left_on) and _any(self.right_on):
            for lk, rk in zip(self.left_on, self.right_on):
                if is_lkey(lk):
                    left_keys.append(lk)
                    if is_rkey(rk):
                        right_keys.append(rk)
                        join_names.append(None)  # what to do?
                    else:
                        if rk is not None:
                            right_keys.append(right._get_label_or_level_values(rk))
                            join_names.append(rk)
                        else:
                            # work-around for merge_asof(right_index=True)
                            right_keys.append(right.index)
                            join_names.append(right.index.name)
                else:
                    if not is_rkey(rk):
                        if rk is not None:
                            right_keys.append(right._get_label_or_level_values(rk))
                        else:
                            # work-around for merge_asof(right_index=True)
                            right_keys.append(right.index)
                        if lk is not None and lk == rk:
                            # avoid key upcast in corner case (length-0)
                            if len(left) > 0:
                                right_drop.append(rk)
                            else:
                                left_drop.append(lk)
                    else:
                        right_keys.append(rk)
                    if lk is not None:
                        left_keys.append(left._get_label_or_level_values(lk))
                        join_names.append(lk)
                    else:
                        # work-around for merge_asof(left_index=True)
                        left_keys.append(left.index)
                        join_names.append(left.index.name)
        elif _any(self.left_on):
            for k in self.left_on:
                if is_lkey(k):
                    left_keys.append(k)
                    join_names.append(None)
                else:
                    left_keys.append(left._get_label_or_level_values(k))
                    join_names.append(k)
            if isinstance(self.right.index, MultiIndex):
                right_keys = [
                    lev._values.take(lev_codes)
                    for lev, lev_codes in zip(
                        self.right.index.levels, self.right.index.codes
                    )
                ]
            else:
                right_keys = [self.right.index._values]
        elif _any(self.right_on):
            for k in self.right_on:
                if is_rkey(k):
                    right_keys.append(k)
                    join_names.append(None)
                else:
                    right_keys.append(right._get_label_or_level_values(k))
                    join_names.append(k)
            if isinstance(self.left.index, MultiIndex):
                left_keys = [
                    lev._values.take(lev_codes)
                    for lev, lev_codes in zip(
                        self.left.index.levels, self.left.index.codes
                    )
                ]
            else:
                left_keys = [self.left.index._values]

        if left_drop:
            self.left = self.left._drop_labels_or_levels(left_drop)

        if right_drop:
            self.right = self.right._drop_labels_or_levels(right_drop)

        return left_keys, right_keys, join_names

    def _maybe_coerce_merge_keys(self):
        # we have valid mergees but we may have to further
        # coerce these if they are originally incompatible types
        #
        # for example if these are categorical, but are not dtype_equal
        # or if we have object and integer dtypes

        for lk, rk, name in zip(
                self.left_join_keys, self.right_join_keys, self.join_names
        ):
            if (len(lk) and not len(rk)) or (not len(lk) and len(rk)):
                continue

            lk_is_cat = is_categorical_dtype(lk)
            rk_is_cat = is_categorical_dtype(rk)
            lk_is_object = is_object_dtype(lk)
            rk_is_object = is_object_dtype(rk)

            # if either left or right is a categorical
            # then the must match exactly in categories & ordered
            if lk_is_cat and rk_is_cat:
                if lk.is_dtype_equal(rk):
                    continue

            elif lk_is_cat or rk_is_cat:
                pass

            elif is_dtype_equal(lk.dtype, rk.dtype):
                continue

            msg = (
                "You are trying to merge on {lk_dtype} and "
                "{rk_dtype} columns. If you wish to proceed "
                "you should use pd.concat".format(lk_dtype=lk.dtype, rk_dtype=rk.dtype)
            )

            # if we are numeric, then allow differing
            # kinds to proceed, eg. int64 and int8, int and float
            # further if we are object, but we infer to
            # the same, then proceed
            if is_numeric_dtype(lk) and is_numeric_dtype(rk):
                if lk.dtype.kind == rk.dtype.kind:
                    continue

                # check whether ints and floats
                elif is_integer_dtype(rk) and is_float_dtype(lk):
                    if not (lk == lk.astype(rk.dtype))[~np.isnan(lk)].all():
                        warnings.warn(
                            "You are merging on int and float "
                            "columns where the float values "
                            "are not equal to their int "
                            "representation",
                            UserWarning,
                        )
                    continue

                elif is_float_dtype(rk) and is_integer_dtype(lk):
                    if not (rk == rk.astype(lk.dtype))[~np.isnan(rk)].all():
                        warnings.warn(
                            "You are merging on int and float "
                            "columns where the float values "
                            "are not equal to their int "
                            "representation",
                            UserWarning,
                        )
                    continue

                # let's infer and see if we are ok
                elif lib.infer_dtype(lk, skipna=False) == lib.infer_dtype(
                        rk, skipna=False
                ):
                    continue

            # Check if we are trying to merge on obviously
            # incompatible dtypes GH 9780, GH 15800

            # bool values are coerced to object
            elif (lk_is_object and is_bool_dtype(rk)) or (
                    is_bool_dtype(lk) and rk_is_object
            ):
                pass

            # object values are allowed to be merged
            elif (lk_is_object and is_numeric_dtype(rk)) or (
                    is_numeric_dtype(lk) and rk_is_object
            ):
                inferred_left = lib.infer_dtype(lk, skipna=False)
                inferred_right = lib.infer_dtype(rk, skipna=False)
                bool_types = ["integer", "mixed-integer", "boolean", "empty"]
                string_types = ["string", "unicode", "mixed", "bytes", "empty"]

                # inferred bool
                if inferred_left in bool_types and inferred_right in bool_types:
                    pass

                # unless we are merging non-string-like with string-like
                elif (
                        inferred_left in string_types and inferred_right not in string_types
                ) or (
                        inferred_right in string_types and inferred_left not in string_types
                ):
                    raise ValueError(msg)

            # datetimelikes must match exactly
            elif needs_i8_conversion(lk) and not needs_i8_conversion(rk):
                raise ValueError(msg)
            elif not needs_i8_conversion(lk) and needs_i8_conversion(rk):
                raise ValueError(msg)
            elif is_datetime64tz_dtype(lk) and not is_datetime64tz_dtype(rk):
                raise ValueError(msg)
            elif not is_datetime64tz_dtype(lk) and is_datetime64tz_dtype(rk):
                raise ValueError(msg)

            elif lk_is_object and rk_is_object:
                continue

            # Houston, we have a problem!
            # let's coerce to object if the dtypes aren't
            # categorical, otherwise coerce to the category
            # dtype. If we coerced categories to object,
            # then we would lose type information on some
            # columns, and end up trying to merge
            # incompatible dtypes. See GH 16900.
            if name in self.left.columns:
                typ = lk.categories.dtype if lk_is_cat else object
                self.left = self.left.assign(**{name: self.left[name].astype(typ)})
            if name in self.right.columns:
                typ = rk.categories.dtype if rk_is_cat else object
                self.right = self.right.assign(**{name: self.right[name].astype(typ)})

    def _validate_specification(self):
        # TODO this makes the on columns not none
        # Hm, any way to make this logic less complicated??
        if self.on is None and self.left_on is None and self.right_on is None:

            if self.left_index and self.right_index:
                self.left_on, self.right_on = (), ()
            elif self.left_index:
                if self.right_on is None:
                    raise MergeError("Must pass right_on or right_index=True")
            elif self.right_index:
                if self.left_on is None:
                    raise MergeError("Must pass left_on or left_index=True")
            else:
                # use the common columns
                common_cols = self.left.columns.intersection(self.right.columns)
                if len(common_cols) == 0:
                    raise MergeError(
                        "No common columns to perform merge on. "
                        "Merge options: left_on={lon}, right_on={ron}, "
                        "left_index={lidx}, right_index={ridx}".format(
                            lon=self.left_on,
                            ron=self.right_on,
                            lidx=self.left_index,
                            ridx=self.right_index,
                        )
                    )
                if not common_cols.is_unique:
                    raise MergeError(f"Data columns not unique: {repr(common_cols)}")
                # TODO get intersection of keys
                self.left_on = self.right_on = common_cols
        elif self.on is not None:
            if self.left_on is not None or self.right_on is not None:
                raise MergeError(
                    'Can only pass argument "on" OR "left_on" '
                    'and "right_on", not a combination of both.'
                )
            self.left_on = self.right_on = self.on
        elif self.left_on is not None:
            n = len(self.left_on)
            if self.right_index:
                if len(self.left_on) != self.right.index.nlevels:
                    raise ValueError(
                        "len(left_on) must equal the number "
                        'of levels in the index of "right"'
                    )
                self.right_on = [None] * n
        elif self.right_on is not None:
            n = len(self.right_on)
            if self.left_index:
                if len(self.right_on) != self.left.index.nlevels:
                    raise ValueError(
                        "len(right_on) must equal the number "
                        'of levels in the index of "left"'
                    )
                self.left_on = [None] * n
        if len(self.right_on) != len(self.left_on):
            raise ValueError("len(right_on) must equal len(left_on)")

    def _validate(self, validate: str):

        # Check uniqueness of each
        if self.left_index:
            left_unique = self.orig_left.index.is_unique
        else:
            left_unique = MultiIndex.from_arrays(self.left_join_keys).is_unique

        if self.right_index:
            right_unique = self.orig_right.index.is_unique
        else:
            right_unique = MultiIndex.from_arrays(self.right_join_keys).is_unique

        # Check data integrity
        if validate in ["one_to_one", "1:1"]:
            if not left_unique and not right_unique:
                raise MergeError(
                    "Merge keys are not unique in either left"
                    " or right dataset; not a one-to-one merge"
                )
            elif not left_unique:
                raise MergeError(
                    "Merge keys are not unique in left dataset;"
                    " not a one-to-one merge"
                )
            elif not right_unique:
                raise MergeError(
                    "Merge keys are not unique in right dataset;"
                    " not a one-to-one merge"
                )

        elif validate in ["one_to_many", "1:m"]:
            if not left_unique:
                raise MergeError(
                    "Merge keys are not unique in left dataset;"
                    " not a one-to-many merge"
                )

        elif validate in ["many_to_one", "m:1"]:
            if not right_unique:
                raise MergeError(
                    "Merge keys are not unique in right dataset;"
                    " not a many-to-one merge"
                )

        elif validate in ["many_to_many", "m:m"]:
            pass

        else:
            raise ValueError("Not a valid argument for validate")


def _get_join_indexers(
        left_keys, right_keys, factorizer, intfactorizer, left_sorter, left_count, sort: bool = False, how: str = "inner", **kwargs
):
    """

    Parameters
    ----------
    left_keys: ndarray, Index, Series
    right_keys: ndarray, Index, Series
    sort: bool, default False
    how: string {'inner', 'outer', 'left', 'right'}, default 'inner'

    Returns
    -------
    tuple of (left_indexer, right_indexer)
        indexers into the left_keys, right_keys

    """
    assert len(left_keys) == len(
        right_keys
    ), "left_key and right_keys must be the same length"
    #print("*********************")
    #print(left_sorter)
    #print(left_count)
    #print("*********************")
    if left_sorter is None and left_count is None:
        #print("need to factorize left and right keys")
        # get left & right join labels and num. of levels at each location
        start = timeit.default_timer()
        mapped = (
            _factorize_keys(left_keys[n], right_keys[n], factorizer, intfactorizer, sort=sort)
            for n in range(len(left_keys))
        )
        end1 = timeit.default_timer()
        print("Time11: ")
        print(end1 - start)
        zipped = zip(*mapped)
        llab, rlab, shape = [list(x) for x in zipped]

        # get flat i8 keys from label lists
        lkey, rkey = _get_join_keys(llab, rlab, shape, factorizer, intfactorizer, sort)

        end2 = timeit.default_timer()
        print("Time13: ")
        print(end2 - start)
        # factorize keys to a dense i8 space
        # `count` is the num. of unique keys
        # set(lkey) | set(rkey) == range(count)
        lkey, rkey, count = _factorize_keys(lkey, rkey, factorizer, intfactorizer, sort=sort)
        end3 = timeit.default_timer()
        print("Time13: ")
        print(end3 - start)
    else:
        #print("need to factorize right keys")
        start = timeit.default_timer()
        mapped = (
            _factorize_right_keys(right_keys[n], factorizer, intfactorizer, sort=sort)
            for n in range(len(right_keys))
        )
        end1 = timeit.default_timer()
        print("Time21: ")
        print(end1 - start)
        zipped = zip(*mapped)
        rlab, shape = [list(x) for x in zipped]
        # get flat i8 keys from label lists
        rkey = _get_right_join_keys(rlab, shape, factorizer, intfactorizer, sort)
        end2 = timeit.default_timer()
        print("Time22: ")
        print(end2 - start)
        rkey, count = _factorize_right_keys(rkey, factorizer, intfactorizer, sort=sort)
        lkey = rkey
        end3 = timeit.default_timer()
        print("Time23: ")
        print(end3 - start)


    # preserve left frame order if how == 'left' and sort == False
    kwargs = copy.copy(kwargs)
    if how == "left":
        kwargs["sort"] = sort
    join_func = _join_functions[how]

    return join_func(lkey, rkey, count, left_sorter, left_count, **kwargs)


def _restore_dropped_levels_multijoin(
        left: MultiIndex,
        right: MultiIndex,
        dropped_level_names,
        join_index,
        lindexer,
        rindexer,
):
    """
    *this is an internal non-public method*

    Returns the levels, labels and names of a multi-index to multi-index join.
    Depending on the type of join, this method restores the appropriate
    dropped levels of the joined multi-index.
    The method relies on lidx, rindexer which hold the index positions of
    left and right, where a join was feasible

    Parameters
    ----------
    left : MultiIndex
        left index
    right : MultiIndex
        right index
    dropped_level_names : str array
        list of non-common level names
    join_index : MultiIndex
        the index of the join between the
        common levels of left and right
    lindexer : intp array
        left indexer
    rindexer : intp array
        right indexer

    Returns
    -------
    levels : list of Index
        levels of combined multiindexes
    labels : intp array
        labels of combined multiindexes
    names : str array
        names of combined multiindexes

    """

    def _convert_to_mulitindex(index) -> MultiIndex:
        if isinstance(index, MultiIndex):
            return index
        else:
            return MultiIndex.from_arrays([index.values], names=[index.name])

    # For multi-multi joins with one overlapping level,
    # the returned index if of type Index
    # Assure that join_index is of type MultiIndex
    # so that dropped levels can be appended
    join_index = _convert_to_mulitindex(join_index)

    join_levels = join_index.levels
    join_codes = join_index.codes
    join_names = join_index.names

    # lindexer and rindexer hold the indexes where the join occurred
    # for left and right respectively. If left/right is None then
    # the join occurred on all indices of left/right
    if lindexer is None:
        lindexer = range(left.size)

    if rindexer is None:
        rindexer = range(right.size)

    # Iterate through the levels that must be restored
    for dropped_level_name in dropped_level_names:
        if dropped_level_name in left.names:
            idx = left
            indexer = lindexer
        else:
            idx = right
            indexer = rindexer

        # The index of the level name to be restored
        name_idx = idx.names.index(dropped_level_name)

        restore_levels = idx.levels[name_idx]
        # Inject -1 in the codes list where a join was not possible
        # IOW indexer[i]=-1
        codes = idx.codes[name_idx]
        restore_codes = algos.take_nd(codes, indexer, fill_value=-1)

        join_levels = join_levels + [restore_levels]
        join_codes = join_codes + [restore_codes]
        join_names = join_names + [dropped_level_name]

    return join_levels, join_codes, join_names


_type_casters = {
    "int64_t": ensure_int64,
    "double": ensure_float64,
    "object": ensure_object,
}


def _get_multiindex_indexer(join_keys, index: MultiIndex, sort: bool):
    # left & right join labels and num. of levels at each location
    mapped = (
        _factorize_keys(index.levels[n], join_keys[n], sort=sort)
        for n in range(index.nlevels)
    )
    zipped = zip(*mapped)
    rcodes, lcodes, shape = [list(x) for x in zipped]
    if sort:
        rcodes = list(map(np.take, rcodes, index.codes))
    else:
        i8copy = lambda a: a.astype("i8", subok=False, copy=True)
        rcodes = list(map(i8copy, index.codes))

    # fix right labels if there were any nulls
    for i in range(len(join_keys)):
        mask = index.codes[i] == -1
        if mask.any():
            # check if there already was any nulls at this location
            # if there was, it is factorized to `shape[i] - 1`
            a = join_keys[i][lcodes[i] == shape[i] - 1]
            if a.size == 0 or not a[0] != a[0]:
                shape[i] += 1

            rcodes[i][mask] = shape[i] - 1

    # get flat i8 join keys
    lkey, rkey = _get_join_keys(lcodes, rcodes, shape, sort)

    # factorize keys to a dense i8 space
    lkey, rkey, count = _factorize_keys(lkey, rkey, sort=sort)

    return libjoin.left_outer_join(lkey, rkey, count, sort=sort)


def _get_single_indexer(join_key, index, sort: bool = False):
    left_key, right_key, count = _factorize_keys(join_key, index, sort=sort)

    left_indexer, right_indexer = libjoin.left_outer_join(
        ensure_int64(left_key), ensure_int64(right_key), count, sort=sort
    )

    return left_indexer, right_indexer


def _left_join_on_index(left_ax: Index, right_ax: Index, join_keys, sort: bool = False):
    if len(join_keys) > 1:
        if not (
                (isinstance(right_ax, MultiIndex) and len(join_keys) == right_ax.nlevels)
        ):
            raise AssertionError(
                "If more than one join key is given then "
                "'right_ax' must be a MultiIndex and the "
                "number of join keys must be the number of "
                "levels in right_ax"
            )

        left_indexer, right_indexer = _get_multiindex_indexer(
            join_keys, right_ax, sort=sort
        )
    else:
        jkey = join_keys[0]

        left_indexer, right_indexer = _get_single_indexer(jkey, right_ax, sort=sort)

    if sort or len(left_ax) != len(left_indexer):
        # if asked to sort or there are 1-to-many matches
        join_index = left_ax.take(left_indexer)
        return join_index, left_indexer, right_indexer

    # left frame preserves order & length of its index
    return left_ax, None, right_indexer


def _right_outer_join(x, y, max_groups):
    right_indexer, left_indexer = libjoin.left_outer_join(y, x, max_groups)
    return left_indexer, right_indexer


_join_functions = {
    "inner": libjoin.inner_join,
    "left": libjoin.left_outer_join,
    "right": _right_outer_join,
    "outer": libjoin.full_outer_join,
    "pipeline": libjoin.pipeline_inner_join,
    "pipeline_merge": libjoin.pipeline_inner_merge_join,
}


def _factorize_keys(lk, rk, objectrizer, intrizer, sort=True):
    # Some pre-processing for non-ndarray lk / rk
    if is_datetime64tz_dtype(lk) and is_datetime64tz_dtype(rk):
        lk = getattr(lk, "_values", lk)._data
        rk = getattr(rk, "_values", rk)._data

    elif (
            is_categorical_dtype(lk) and is_categorical_dtype(rk) and lk.is_dtype_equal(rk)
    ):
        if lk.categories.equals(rk.categories):
            # if we exactly match in categories, allow us to factorize on codes
            rk = rk.codes
        else:
            # Same categories in different orders -> recode
            rk = _recode_for_categories(rk.codes, rk.categories, lk.categories)

        lk = ensure_int64(lk.codes)
        rk = ensure_int64(rk)

    elif (
            is_extension_array_dtype(lk.dtype)
            and is_extension_array_dtype(rk.dtype)
            and lk.dtype == rk.dtype
    ):
        lk, _ = lk._values_for_factorize()
        rk, _ = rk._values_for_factorize()

    if is_integer_dtype(lk) and is_integer_dtype(rk):
        # GH#23917 TODO: needs tests for case where lk is integer-dtype
        #  and rk is datetime-dtype
        klass = libhashtable.Int64Factorizer
        flag = 0
        lk = ensure_int64(com.values_from_object(lk))
        rk = ensure_int64(com.values_from_object(rk))
    elif issubclass(lk.dtype.type, (np.timedelta64, np.datetime64)) and issubclass(
            rk.dtype.type, (np.timedelta64, np.datetime64)
    ):
        # GH#23917 TODO: Needs tests for non-matching dtypes
        klass = libhashtable.Int64Factorizer
        flag = 0
        lk = ensure_int64(com.values_from_object(lk))
        rk = ensure_int64(com.values_from_object(rk))
    else:
        klass = libhashtable.Factorizer
        flag = 1
        lk = ensure_object(lk)
        rk = ensure_object(rk)

    #rizer = klass(max(len(lk), len(rk)))
    if flag == 0:
        rizer = intrizer
    else:
        rizer = objectrizer
    llab = rizer.factorize(lk)
    rlab = rizer.factorize(rk)
    #print("$$$$$$$$$$$$$$$$$$$$$$")
    #print(lk)
    #print(rk)
    #print(llab)
    #print(rlab)
    #print("$$$$$$$$$$$$$$$$$$$$$$")
    count = rizer.get_count()

    if sort:
        uniques = rizer.uniques.to_array()
        llab, rlab = _sort_labels(uniques, llab, rlab)

    # NA group
    lmask = llab == -1
    lany = lmask.any()
    rmask = rlab == -1
    rany = rmask.any()

    if lany or rany:
        if lany:
            np.putmask(llab, lmask, count)
        if rany:
            np.putmask(rlab, rmask, count)
        count += 1

    return llab, rlab, count

def _factorize_right_keys(rk, objectrizer, intrizer, sort=True):
    # Some pre-processing for non-ndarray lk / rk
    if is_datetime64tz_dtype(rk):
        rk = getattr(rk, "_values", rk)._data

    elif (
            is_categorical_dtype(rk)
    ):
        rk = rk.codes
        rk = ensure_int64(rk)

    elif (
        is_extension_array_dtype(rk.dtype)
    ):
        rk, _ = rk._values_for_factorize()

    if is_integer_dtype(rk):
        # GH#23917 TODO: needs tests for case where lk is integer-dtype
        #  and rk is datetime-dtype
        klass = libhashtable.Int64Factorizer
        flag = 0
        rk = ensure_int64(com.values_from_object(rk))
    elif issubclass(
            rk.dtype.type, (np.timedelta64, np.datetime64)
    ):
        # GH#23917 TODO: Needs tests for non-matching dtypes
        klass = libhashtable.Int64Factorizer
        flag = 0
        rk = ensure_int64(com.values_from_object(rk))
    else:
        klass = libhashtable.Factorizer
        flag = 1
        rk = ensure_object(rk)

    #rizer = klass(max(len(lk), len(rk)))
    if flag == 0:
        rizer = intrizer
    else:
        rizer = objectrizer
    rlab = rizer.factorize(rk)
    #print("$$$$$$$$$$$$$$$$$$$$$$")
    #print(lk)
    #print(rk)
    #print(llab)
    #print(rlab)
    #print("$$$$$$$$$$$$$$$$$$$$$$")
    count = rizer.get_count()

    rmask = rlab == -1
    rany = rmask.any()

    if rany:
        np.putmask(rlab, rmask, count)
        count += 1

    return rlab, count

def _sort_labels(uniques: np.ndarray, left, right):
    if not isinstance(uniques, np.ndarray):
        # tuplesafe
        uniques = Index(uniques).values

    llength = len(left)
    labels = np.concatenate([left, right])

    _, new_labels = algos.safe_sort(uniques, labels, na_sentinel=-1)
    new_labels = ensure_int64(new_labels)
    new_left, new_right = new_labels[:llength], new_labels[llength:]

    return new_left, new_right


def _get_join_keys(llab, rlab, shape, factorizer, intfactorizer, sort: bool):
    # FIXME 6
    # how many levels can be done without overflow
    pred = lambda i: not is_int64_overflow_possible(shape[:i])
    nlev = next(filter(pred, range(len(shape), 0, -1)))

    # get keys for the first `nlev` levels
    stride = np.prod(shape[1:nlev], dtype="i8")
    lkey = stride * llab[0].astype("i8", subok=False, copy=False)
    rkey = stride * rlab[0].astype("i8", subok=False, copy=False)

    for i in range(1, nlev):
        with np.errstate(divide="ignore"):
            stride //= shape[i]
        lkey += llab[i] * stride
        rkey += rlab[i] * stride

    if nlev == len(shape):  # all done!
        return lkey, rkey

    # densify current keys to avoid overflow
    lkey, rkey, count = _factorize_keys(lkey, rkey, factorizer, intfactorizer, sort=sort)

    llab = [lkey] + llab[nlev:]
    rlab = [rkey] + rlab[nlev:]
    shape = [count] + shape[nlev:]

    return _get_join_keys(llab, rlab, shape, factorizer, intfactorizer, sort)

def _get_right_join_keys(rlab, shape, factorizer, intfactorizer, sort: bool):
    # FIXME 6
    # how many levels can be done without overflow
    pred = lambda i: not is_int64_overflow_possible(shape[:i])
    nlev = next(filter(pred, range(len(shape), 0, -1)))

    # get keys for the first `nlev` levels
    stride = np.prod(shape[1:nlev], dtype="i8")
    rkey = stride * rlab[0].astype("i8", subok=False, copy=False)

    for i in range(1, nlev):
        with np.errstate(divide="ignore"):
            stride //= shape[i]
        rkey += rlab[i] * stride

    if nlev == len(shape):  # all done!
        return rkey

    # densify current keys to avoid overflow
    rkey, count = _factorize_right_keys(rkey, factorizer, intfactorizer, sort=sort)
    rlab = [rkey] + rlab[nlev:]
    shape = [count] + shape[nlev:]

    return _get_right_join_keys(rlab, shape, factorizer, intfactorizer, sort)


def _should_fill(lname, rname) -> bool:
    if not isinstance(lname, str) or not isinstance(rname, str):
        return True
    return lname == rname


def _any(x) -> bool:
    return x is not None and com.any_not_none(*x)


def _validate_operand(obj: FrameOrSeries) -> "DataFrame":
    if isinstance(obj, ABCDataFrame):
        return obj
    elif isinstance(obj, ABCSeries):
        if obj.name is None:
            raise ValueError("Cannot merge a Series without a name")
        else:
            return obj.to_frame()
    else:
        raise TypeError(
            "Can only merge Series or DataFrame objects, "
            "a {obj} was passed".format(obj=type(obj))
        )


def _items_overlap_with_suffix(left: Index, lsuffix, right: Index, rsuffix):
    """
    If two indices overlap, add suffixes to overlapping entries.

    If corresponding suffix is empty, the entry is simply converted to string.

    """
    to_rename = left.intersection(right)
    if len(to_rename) == 0:
        return left, right

    if not lsuffix and not rsuffix:
        raise ValueError(
            "columns overlap but no suffix specified: "
            "{rename}".format(rename=to_rename)
        )

    def renamer(x, suffix):
        """
        Rename the left and right indices.

        If there is overlap, and suffix is not None, add
        suffix, otherwise, leave it as-is.

        Parameters
        ----------
        x : original column name
        suffix : str or None

        Returns
        -------
        x : renamed column name
        """
        if x in to_rename and suffix is not None:
            return "{x}{suffix}".format(x=x, suffix=suffix)
        return x

    lrenamer = partial(renamer, suffix=lsuffix)
    rrenamer = partial(renamer, suffix=rsuffix)

    return (_transform_index(left, lrenamer), _transform_index(right, rrenamer))
