CREATE OR REPLACE TABLE DH_DATABASE.dh_prod.segments_derived AS
             with peru_data                  AS
             (       
              /*This block fetch only the Peru data */
                    SELECT DATE('2020-06-15') AS cur_date,
                           *
                    FROM   DH_RAW.OLD_VOUCHER_DATA
                    WHERE  1=1
                    AND    lower(country_code) = 'peru'
                    AND    total_orders <> ''
                    AND    voucher_amount <>''
             )
             ,
             date_diff_ts AS
             (
              /* This block will find the difference from last date to the current date */
                    SELECT *,
                           (cur_date::DATE - last_order_ts::DATE) AS datediff_last_order
                    FROM   peru_data
             )
             ,
             invalid_entries AS
             (
              /*
                     This block will find all the invalid entires from the data.
                     The following are the checks performed.
                     1. total_orders are 0
                     2. last_order_ts is lower than first_order_ts
                     3. blank values in total_orders column
                     4. voucher_amount column with blank values
              */
                    SELECT *,
                           CASE
                                  WHEN total_orders = 0
                                  AND    (
                                                last_order_ts IS NOT NULL
                                         OR     first_order_ts IS NOT NULL) THEN TRUE
                                  WHEN last_order_ts < first_order_ts THEN TRUE
                                  ELSE FALSE
                           END AS invalid
                    FROM   date_diff_ts
             )
             ,
             segs AS
             (       
              /*
                     This block of code will identify all the segments.
                     The data for this condition is part of the requirements.
                     Only Valid entires will be considered and invalid will be ignored
              */
                    SELECT *,
                           CASE
                                  WHEN total_orders BETWEEN 1 AND    4 THEN '1-4'
                                  WHEN total_orders BETWEEN 5 AND    13 THEN '5-13'
                                  WHEN total_orders BETWEEN 14 AND    37 THEN '14-37'
                                  WHEN total_orders > 37 THEN 'older'
                           END AS ferquency_segment,
                           CASE
                                  WHEN datediff_last_order BETWEEN 30 AND    60 THEN '30-60'
                                  WHEN datediff_last_order BETWEEN 61 AND    90 THEN '61-90'
                                  WHEN datediff_last_order BETWEEN 91 AND    120 THEN '91-120'
                                  WHEN datediff_last_order BETWEEN 121 AND    180 THEN '121-180'
                                  WHEN datediff_last_order > 180 THEN '180+'
                           END AS recency_segment
                    FROM   invalid_entries
                    WHERE  invalid= FALSE
             )
             ,
             most_used_voucher_values_frequent AS
             (
              /* This block calculates the most used voucher values based on given frequencies */
                      SELECT   'ferquency_segment' AS segment_type,
                               ferquency_segment,
                               voucher_amount,
                               count(*) AS count_occurrences
                      FROM     segs
                      GROUP BY 1,
                               2,
                               3
             )
             ,
             most_used_voucher_values_frequent_partition AS
             (
              /* This block calculates the most used voucher values for given frequencies  based on window function */
                      SELECT   *,
                               row_number() over (PARTITION BY ferquency_segment ORDER BY count_occurrences DESC) AS row_num
                      FROM     most_used_voucher_values_frequent
             )
             ,
             most_used_voucher_values_recency AS
             (
              /* This block calculates the most used voucher values based on given recency windows */
                      SELECT   'recency_segment' AS segment_type,
                               recency_segment,
                               voucher_amount,
                               count(*) AS count_occurrences
                      FROM     segs
                      GROUP BY 1,
                               2,
                               3
             )
             ,
             most_used_voucher_values_recency_partition AS
             (
              /* This block calculates the most used voucher values for given recnecy windows based on window function */
                      SELECT   *,
                               row_number() over (PARTITION BY recency_segment ORDER BY count_occurrences DESC) AS row_num
                      FROM     most_used_voucher_values_recency
             )
             ,
             unioned_dataset AS
             (
              /* This block will union all the freq & recency windows for making segments */
                    SELECT segment_type,
                           ferquency_segment AS segment_name,
                           voucher_amount,
                           count_occurrences
                    FROM   most_used_voucher_values_frequent_partition
                    WHERE  row_num = 1
                    UNION ALL
                    SELECT segment_type,
                           recency_segment AS segment_name,
                           voucher_amount,
                           count_occurrences
                    FROM   most_used_voucher_values_recency_partition
                    WHERE  row_num = 1
             )
       /* This query will join the union data will the rules we have got from the requirements */
      SELECT    t1.segment_type,
                t1.segment_name,
                lower_value,
                upper_value,
                voucher_amount
      FROM      unioned_dataset t1
      left join dh_stage.rules_for_segments t2
      ON        t1.segment_type = t2.segment_type
      AND       t1.segment_name = t2.segment_name;