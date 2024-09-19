from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, md5, col, current_date, current_timestamp, lit, date_sub

#from utils.logger import Logger
#from utils.spark_session import SparkSessionManager

run_date = date_sub(current_date(), 1)

class SCDHandler:
    # TODO : Add logging for the different steps. Especially coutns on the different data sets, times, and possible errors.
    def check_columns_presence(self, source_df, target_df, metadata_cols):
        """
        Check if all columns from the target DataFrame are present in the source DataFrame.

        Args:
            source_df (pyspark.sql.DataFrame): Source DataFrame.
            target_df (pyspark.sql.DataFrame): Target DataFrame.

        Raises:
            Exception: If columns are missing in the source DataFrame.

        Returns:
            None
        """
        cols_missing = set([cols for cols in target_df.columns if cols not in source_df.columns]) - set(metadata_cols)
        if cols_missing:
            raise Exception(f"Cols missing in source DataFrame: {cols_missing}")

    def apply_hash_and_alias(self, source_df, target_df, metadata_cols) -> ([DataFrame, DataFrame]):
        """
        Apply hash calculation and alias to source and target DataFrames.

        Args:
            source_df (pyspark.sql.DataFrame): Source DataFrame.
            target_df (pyspark.sql.DataFrame): Target DataFrame.
            metadata_cols (list): List of metadata columns to exclude from hash calculation.

        Returns:
            tuple: Tuple containing aliased source DataFrame and aliased target DataFrame.
        """
        # Extract columns from target DataFrame excluding metadata columns
        tgt_cols = [x for x in target_df.columns if x not in metadata_cols]

        # Calculate hash expression
        hash_expr = md5(concat_ws("|", *[col(c) for c in tgt_cols]))

        # NB! TODO : Comparing different datatypes can result in change in hash, e.g. 15 vs 15.000. Probably safe to 
        # have a temp table that stores incoming data, e.g. result of view/sql, and 
        

        # Apply hash calculation and alias to source and target DataFrames
        source_df = source_df.withColumn("hash_value", hash_expr).alias("source_df")
        target_df = target_df.withColumn("hash_value", hash_expr).alias("target_df")

        return source_df, target_df

    def scd_2(self, source_df, target_df, join_keys, metadata_cols=None) -> DataFrame:
        if metadata_cols is None:
            metadata_cols = ['scd_active', 'scd_from', 'scd_to', 'sys_loaded_time', 'sys_updated_time']
        tgt_cols = [x for x in target_df.columns]
        self.check_columns_presence(source_df, target_df, metadata_cols)
        # Apply hash calculation and alias
        source_df, target_df = self.apply_hash_and_alias(source_df, target_df, metadata_cols)

        # Split target into inactive and active
        inactive_df = target_df.filter("target_df.scd_active = 0")
        active_df = target_df.filter("target_df.scd_active = 1")

        # Identify new records
        join_cond = [source_df[join_key] == active_df[join_key] for join_key in join_keys]
        new_df = source_df.join(active_df, join_cond, 'left_anti')

        base_df = active_df.join(source_df, join_cond, 'left')

        # Filter unchanged or updated records
        key_match_expr = " and ".join([f"source_df.{key} IS NOT NULL" for key in join_keys])
        unchanged_df = base_df.filter(f"({key_match_expr}) AND "
                                      f"(source_df.hash_value = target_df.hash_value)") \
            .select("target_df.*")

        # identify updated records
        updated_df = base_df.filter(f"{key_match_expr} AND "
                                    f"source_df.hash_value != target_df.hash_value")

        # Identify deleted records
        deleted_expr = " AND ".join([f"source_df.{key} IS NULL" for key in join_keys])
        deleted_df = base_df.filter(f"{deleted_expr}")

        # pick updated records from source_df for new entry
        updated_new_df = updated_df.select("source_df.*")

        # pick updated records from target_df for obsolete entry
        obsolete_df = updated_df.select("target_df.*").unionByName(deleted_df.select("target_df.*")) \
            .withColumn("scd_to", date_sub(run_date, 1)) \
            .withColumn("scd_active", lit(0)) \
            .withColumn("sys_updated_time", current_timestamp())

        # union : new & updated records and add scd2 meta-deta
        delta_df = new_df.union(updated_new_df) \
            .withColumn("scd_from", run_date) \
            .withColumn("scd_to", lit("9999-12-24").cast("date")) \
            .withColumn("scd_active", lit(1)) \
            .withColumn("sys_loaded_time", current_timestamp()) \
            .withColumn("sys_updated_time", current_timestamp())

        # union all datasets : delta_df + obsolete_df + unchanged_df + inactive_df
        result_df = unchanged_df.select(tgt_cols). \
            unionByName(delta_df.select(tgt_cols)). \
            unionByName(obsolete_df.select(tgt_cols)). \
            unionByName(inactive_df.select(tgt_cols))

        return result_df