#include <Processors/TTL/ITTLAlgorithm.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Common/DateLUTImpl.h>
#include <Interpreters/ExpressionActions.h>

#include <Columns/ColumnsDateTime.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_TTL_EXPRESSION;
}

namespace
{
    /// Same type dispatch as ITTLAlgorithm::getTimestampByIndex, but as a free function
    /// so it can be reused from the static `checkOverflow` helper.
    Int64 extractTimestamp(const IColumn * column, size_t index, const DateLUTImpl & date_lut)
    {
        if (const auto * col_sparse = typeid_cast<const ColumnSparse *>(column))
            return extractTimestamp(&col_sparse->getValuesColumn(), col_sparse->getValueIndex(index), date_lut);

        if (const auto * col_date = typeid_cast<const ColumnUInt16 *>(column))
            return date_lut.fromDayNum(DayNum(col_date->getData()[index]));
        if (const auto * col_date_time = typeid_cast<const ColumnUInt32 *>(column))
            return col_date_time->getData()[index];
        if (const auto * col_date_32 = typeid_cast<const ColumnInt32 *>(column))
            return date_lut.fromDayNum(ExtendedDayNum(col_date_32->getData()[index]));
        if (const auto * col_date_time_64 = typeid_cast<const ColumnDateTime64 *>(column))
            return col_date_time_64->getData()[index] / intExp10OfSize<Int64>(col_date_time_64->getScale());

        if (const auto * col_const = typeid_cast<const ColumnConst *>(column))
        {
            const auto & inner = col_const->getDataColumn();
            if (typeid_cast<const ColumnUInt16 *>(&inner))
                return date_lut.fromDayNum(DayNum(col_const->getValue<UInt16>()));
            if (typeid_cast<const ColumnUInt32 *>(&inner))
                return col_const->getValue<UInt32>();
            if (typeid_cast<const ColumnInt32 *>(&inner))
                return date_lut.fromDayNum(ExtendedDayNum(col_const->getValue<Int32>()));
            if (const auto * inner_dt64 = typeid_cast<const ColumnDateTime64 *>(&inner))
                return col_const->getValue<DateTime64>() / intExp10OfSize<Int64>(inner_dt64->getScale());
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type of result TTL column");
    }
}

ITTLAlgorithm::ITTLAlgorithm(
    const TTLExpressions & ttl_expressions_, const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_)
    : ttl_expressions(ttl_expressions_)
    , description(description_)
    , old_ttl_info(old_ttl_info_)
    , current_time(current_time_)
    , force(force_)
    , date_lut(DateLUT::instance())
{
}

bool ITTLAlgorithm::isTTLExpired(time_t ttl) const
{
    return (ttl && (ttl <= current_time));
}

ColumnPtr ITTLAlgorithm::executeExpressionAndGetColumn(
    const ExpressionActionsPtr & expression, const Block & block, const String & result_column)
{
    if (!expression)
        return nullptr;

    if (block.has(result_column))
        return block.getByName(result_column).column;

    Block block_copy;
    for (const auto & column_name : expression->getRequiredColumns())
        block_copy.insert(block.getByName(column_name));

    /// Keep number of rows for const expression.
    size_t num_rows = block.rows();
    expression->execute(block_copy, num_rows);

    return block_copy.getByName(result_column).column;
}

/// TODO: This per-row type dispatch is inefficient when called in a loop.
/// Callers should resolve the column type once and iterate over typed data directly.
/// See TTLDeleteFilterTransform::extractTimestamps for a batch-oriented approach.
Int64 ITTLAlgorithm::getTimestampByIndex(const IColumn * column, size_t index) const
{
    return extractTimestamp(column, index, date_lut);
}

void ITTLAlgorithm::checkOverflow(
    const ExpressionActionsPtr & overflow_check_expression,
    const ColumnPtr & original_ttl_column,
    const String & result_column,
    const Block & block)
{
    if (!overflow_check_expression || !original_ttl_column)
        return;

    const size_t num_rows = block.rows();
    if (num_rows == 0)
        return;

    Block block_copy;
    for (const auto & column_name : overflow_check_expression->getRequiredColumns())
        block_copy.insert(block.getByName(column_name));

    size_t rows = num_rows;
    overflow_check_expression->execute(block_copy, rows);
    auto widened_column = block_copy.getByName(result_column).column;

    const auto & date_lut = DateLUT::instance();
    for (size_t i = 0; i < num_rows; ++i)
    {
        Int64 original_ts = extractTimestamp(original_ttl_column.get(), i, date_lut);
        Int64 widened_ts = extractTimestamp(widened_column.get(), i, date_lut);
        if (original_ts != widened_ts)
        {
            throw Exception(ErrorCodes::BAD_TTL_EXPRESSION,
                "TTL expression result overflowed at row {}: narrow evaluation produced {} "
                "but the widened evaluation produced {}. This indicates the TTL interval "
                "exceeds the range of the input type. Consider using Date32 or DateTime64 "
                "for columns referenced by the TTL expression.",
                i, original_ts, widened_ts);
        }
    }
}

}
