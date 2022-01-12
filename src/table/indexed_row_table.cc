#include "indexed_row_table.h"
#include <cstring>

namespace bytedance_db_project
{
  IndexedRowTable::IndexedRowTable(int32_t index_column)
  {
    index_column_ = index_column;
  }

  void IndexedRowTable::Load(BaseDataLoader *loader)
  {
    // TODO: Implement this!
    num_cols_ = loader->GetNumCols();
    auto rows = loader->GetRows();
    num_rows_ = rows.size();
    rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
    for (auto row_id = 0; row_id < num_rows_; row_id++)
    {
      auto cur_row = rows.at(row_id);
      std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                  FIXED_FIELD_LEN * num_cols_);

      //给index_column_添加索引
      auto pos = rows_ + FIXED_FIELD_LEN * ((row_id * num_cols_) + index_column_);
      int32_t key = *(int32_t *)pos;
      auto iter = index_.find(key);
      if (iter == index_.end())
      { //map中没有，添加
        std::vector<int32_t> row_id_list(1, row_id);
        index_.insert(std::make_pair(key, row_id_list));
      }
      else
      { //map中有，rowid合并到vector中即可
        (iter->second).push_back(row_id);
      }
    }
  }

  int32_t IndexedRowTable::GetIntField(int32_t row_id, int32_t col_id)
  {
    // TODO: Implement this!
    //N*M:  B*((i*M)+j)
    auto offset = FIXED_FIELD_LEN * ((row_id * num_cols_) + col_id);
    return *(int32_t *)(rows_ + offset);
  }

  void IndexedRowTable::PutIntField(int32_t row_id, int32_t col_id,
                                    int32_t field)
  {
    // TODO: Implement this!
    auto offset = FIXED_FIELD_LEN * ((row_id * num_cols_) + col_id);
    *(int32_t *)(rows_ + offset) = field;
  }

  int64_t IndexedRowTable::ColumnSum()
  {
    // TODO: Implement this!
    // SELECT SUM(col0) FROM table;
    int64_t sum = 0;
    for (auto iter = index_.begin(); iter != index_.end(); iter++)
    {
      sum = sum + (iter->first) * (iter->second.size());
    }
    return sum;
  }

  int64_t IndexedRowTable::PredicatedColumnSum(int32_t threshold1,
                                               int32_t threshold2)
  {
    // TODO: Implement this!
    // SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
    int64_t sum = 0;
    for (auto row_id = 0; row_id < num_rows_; row_id++)
    {
      int32_t col1 = GetIntField(row_id, 1);
      int32_t col2 = GetIntField(row_id, 2);
      if (col1 > threshold1 && col2 < threshold2)
      {
        int32_t col0 = GetIntField(row_id, 0);
        sum = sum + col0;
      }
    }
    return sum;
  }

  int64_t IndexedRowTable::PredicatedAllColumnsSum(int32_t threshold)
  {
    // TODO: Implement this!
    // SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 >
    int64_t sum = 0;
    auto iter = index_.upper_bound(threshold); //第一个>thre的值在树中的位置，由于map按key排序，后续均>thre
    for (; iter != index_.end(); iter++)
    {
      //直接从map中获取col0的值对应的行,处理这些行即可，避免遍历所有行
      for (int32_t &row_id : iter->second)
      {
        auto offset = FIXED_FIELD_LEN * (row_id * num_cols_);
        //转为int32_t类型指针，方便使用指针++操作
        int32_t *p = (int32_t *)(rows_ + offset);
        for (auto col_id = 0; col_id < num_cols_; col_id++)
        {
          sum = sum + *p;
          p++;
        }
      }
    }
    return sum;
  }

  int64_t IndexedRowTable::PredicatedUpdate(int32_t threshold)
  {
    // TODO: Implement this!
    // UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
    int64_t update_rows_nums = 0;
    //map中只有lower_bound(>=thre)和upper_bound(>thre),没有<thre
    //可以取反求>=的值的位置,其前一个位置是满足要求的最大的值，反向迭代即为所求
    auto iter = index_.lower_bound(threshold);
    // 利用iter构造反向迭代器，注意！反向迭代器构造出的是iter前一个位置！
    std::map<int32_t, std::vector<int32_t>>::reverse_iterator riter(iter);
    for (; riter != index_.rend(); riter++)
    {
      for (int32_t &row_id : riter->second)
      {
        int32_t new_col3 = GetIntField(row_id, 2) + GetIntField(row_id, 3);
        PutIntField(row_id, 3, new_col3);
        update_rows_nums++;
      }
    }
    return update_rows_nums;
  }
} // namespace bytedance_db_project