#include "row_table.h"
#include <cstring>

namespace bytedance_db_project
{
  RowTable::RowTable() {}

  RowTable::~RowTable()
  {
    if (rows_ != nullptr)
    {
      delete rows_;
      rows_ = nullptr;
    }
  }

  void RowTable::Load(BaseDataLoader *loader)
  {
    num_cols_ = loader->GetNumCols();
    auto rows = loader->GetRows();
    num_rows_ = rows.size();
    // 所有数组存成一行（一维数组）
    rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
    for (auto row_id = 0; row_id < num_rows_; row_id++)
    {
      auto cur_row = rows.at(row_id);
      std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                  FIXED_FIELD_LEN * num_cols_); //复制数据
    }
  }

  int32_t RowTable::GetIntField(int32_t row_id, int32_t col_id)
  {
    // TODO: Implement this!
    //N*M:  B*((i*M)+j)
    auto offset = FIXED_FIELD_LEN * ((row_id * num_cols_) + col_id);
    return *(int32_t *)(rows_ + offset);
  }

  void RowTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field)
  {
    // TODO: Implement this!
    auto offset = FIXED_FIELD_LEN * ((row_id * num_cols_) + col_id);
    *(int32_t *)(rows_ + offset) = field;
  }

  int64_t RowTable::ColumnSum()
  {
    // TODO: Implement this!
    // SELECT SUM(col0) FROM table;
    int64_t sum = 0;
    for (auto row_id = 0; row_id < num_rows_; row_id++)
    {
      auto offset = FIXED_FIELD_LEN * ((row_id * num_cols_) + 0);
      int32_t col0_at_row = *(int32_t *)(rows_ + offset);
      sum = sum + col0_at_row;
    }
    return sum;
  }

  int64_t RowTable::PredicatedColumnSum(int32_t threshold1, int32_t threshold2)
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

  int64_t RowTable::PredicatedAllColumnsSum(int32_t threshold)
  {
    // TODO: Implement this!
    // SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 >
    int64_t sum = 0;
    for (auto row_id = 0; row_id < num_rows_; row_id++)
    {
      auto offset = FIXED_FIELD_LEN * ((row_id * num_cols_) + 0);
      //转为int32_t类型指针，方便使用指针++操作
      int32_t *p = (int32_t *)(rows_ + offset);
      if (*p > threshold)
      {
        for (auto col_id = 0; col_id < num_cols_; col_id++)
        {
          sum = sum + *p;
          p++;
        }
      }
    }
    return sum;
  }

  int64_t RowTable::PredicatedUpdate(int32_t threshold)
  {
    // TODO: Implement this!
    // UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
    int64_t update_rows_nums = 0;
    for (auto row_id = 0; row_id < num_rows_; row_id++)
    {
      int32_t col0 = GetIntField(row_id, 0);
      if (col0 < threshold)
      {
        int32_t new_col3 = GetIntField(row_id, 2) + GetIntField(row_id, 3);
        PutIntField(row_id, 3, new_col3);
        update_rows_nums++;
      }
    }
    return update_rows_nums;
  }
} // namespace bytedance_db_project