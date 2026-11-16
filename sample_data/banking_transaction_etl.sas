/*
  Synthetic Banking Transaction ETL - SAS implementation
  Purpose: normalize, validate, enrich, deduplicate and aggregate transactions.
  Companion file: banking_transaction_etl_pyspark.py
  Input data is synthetic. Paths and credentials are placeholders.
  The paired implementations are intended for semantic migration comparison.
*/
options mprint mlogic symbolgen;
options validvarname=any;

%let INPUT_PATH=/data/input/transactions;
%let OUTPUT_PATH=/data/output;
%let RUN_DATE=%sysfunc(today());
%let AMOUNT_TOLERANCE=0.005;
%let DUPLICATE_KEEP=LATEST;

libname raw   "&INPUT_PATH";
libname stage "&OUTPUT_PATH./stage";
libname mart  "&OUTPUT_PATH./mart";

%macro log_step(step_name);
  %put NOTE: ==================================================;
  %put NOTE: STARTING STEP=&step_name;
  %put NOTE: RUN_DATE=&RUN_DATE;
  %put NOTE: ==================================================;
%mend;

%macro assert_dataset(ds=);
  %if not %sysfunc(exist(&ds)) %then %do;
    %put ERROR: Required dataset &ds does not exist.;
    %abort cancel;
  %end;
%mend;

%macro count_rows(ds=, outvar=);
  proc sql noprint;
    select count(*) into :&outvar trimmed from &ds;
  quit;
  %put NOTE: DATASET=&ds ROW_COUNT=&&&outvar;
%mend;

%macro normalize_text(in=, out=);
  data &out;
    set &in;
    array chars {*} _character_;
    do _i=1 to dim(chars);
      chars{_i}=strip(chars{_i});
    end;
    drop _i;
  run;
%mend;

%macro reject_rows(in=, out=, rejected=);
  data &out &rejected;
    set &in;
    length rejection_reason $500;
    rejection_reason='';
    if missing(customer_id) then rejection_reason=catx('|',rejection_reason,'MISSING_CUSTOMER_ID');
    if missing(account_id) then rejection_reason=catx('|',rejection_reason,'MISSING_ACCOUNT_ID');
    if missing(transaction_id) then rejection_reason=catx('|',rejection_reason,'MISSING_TRANSACTION_ID');
    if missing(transaction_date) then rejection_reason=catx('|',rejection_reason,'MISSING_TRANSACTION_DATE');
    if missing(amount) then rejection_reason=catx('|',rejection_reason,'MISSING_AMOUNT');
    if missing(currency) then rejection_reason=catx('|',rejection_reason,'MISSING_CURRENCY');
    if missing(status) then rejection_reason=catx('|',rejection_reason,'MISSING_STATUS');
    if rejection_reason='' then output &out;
    else output &rejected;
  run;
%mend;

%log_step(LOAD_SOURCE);
data stage.transactions_raw;
  infile "&INPUT_PATH./transactions.csv" dsd firstobs=2 truncover;
  length customer_id account_id transaction_id $64;
         currency country_code channel product_code status $16;
         source_system batch_id $40;
         reversal_flag $5;
  informat transaction_date posting_date yymmdd10.;
  informat load_timestamp e8601dt26.6;
  input customer_id :$64.
        account_id :$64.
        transaction_id :$64.
        transaction_date :yymmdd10.
        posting_date :yymmdd10.
        amount :comma20.4
        currency :$16.
        country_code :$16.
        channel :$16.
        product_code :$16.
        status :$16.
        reversal_flag :$5.
        fee_amount :comma20.4
        tax_amount :comma20.4
        net_amount :comma20.4
        source_system :$40.
        batch_id :$40.
        load_timestamp :e8601dt26.6;
  format transaction_date posting_date yymmdd10.;
  format load_timestamp e8601dt26.6;
run;

%assert_dataset(ds=stage.transactions_raw);
%count_rows(ds=stage.transactions_raw,outvar=RAW_COUNT);

%log_step(NORMALIZE_CORE_FIELDS);
data stage.transactions_normalized;
  set stage.transactions_raw;
  customer_id=strip(customer_id);
  account_id=strip(account_id);
  transaction_id=strip(transaction_id);
  currency=upcase(strip(currency));
  country_code=upcase(strip(country_code));
  channel=upcase(strip(channel));
  product_code=upcase(strip(product_code));
  status=upcase(strip(status));
  source_system=upcase(strip(source_system));
  batch_id=strip(batch_id);
  reversal_flag=upcase(strip(reversal_flag));
  if reversal_flag in ('Y','YES','TRUE','1') then reversal_flag='Y';
  else if reversal_flag in ('N','NO','FALSE','0') then reversal_flag='N';
  else if missing(reversal_flag) then reversal_flag='N';
  else reversal_flag='INVALID';
  if missing(fee_amount) then fee_amount=0;
  if missing(tax_amount) then tax_amount=0;
  if missing(net_amount) then net_amount=amount-fee_amount-tax_amount;
  if missing(posting_date) then posting_date=transaction_date;
  if missing(load_timestamp) then load_timestamp=datetime();
  format load_timestamp e8601dt26.6;
run;

%log_step(FIELD_LEVEL_NORMALIZATION);
data stage.transactions_normalized;
  set stage.transactions_normalized;
  ATTRIBUTE_001=strip(ATTRIBUTE_001);
  ATTRIBUTE_002=strip(ATTRIBUTE_002);
  ATTRIBUTE_003=strip(ATTRIBUTE_003);
  ATTRIBUTE_004=strip(ATTRIBUTE_004);
  ATTRIBUTE_005=strip(ATTRIBUTE_005);
  ATTRIBUTE_006=strip(ATTRIBUTE_006);
  ATTRIBUTE_007=strip(ATTRIBUTE_007);
  ATTRIBUTE_008=strip(ATTRIBUTE_008);
  ATTRIBUTE_009=strip(ATTRIBUTE_009);
  ATTRIBUTE_010=strip(ATTRIBUTE_010);
  ATTRIBUTE_011=strip(ATTRIBUTE_011);
  ATTRIBUTE_012=strip(ATTRIBUTE_012);
  ATTRIBUTE_013=strip(ATTRIBUTE_013);
  ATTRIBUTE_014=strip(ATTRIBUTE_014);
  ATTRIBUTE_015=strip(ATTRIBUTE_015);
  ATTRIBUTE_016=strip(ATTRIBUTE_016);
  ATTRIBUTE_017=strip(ATTRIBUTE_017);
  ATTRIBUTE_018=strip(ATTRIBUTE_018);
  ATTRIBUTE_019=strip(ATTRIBUTE_019);
  ATTRIBUTE_020=strip(ATTRIBUTE_020);
  ATTRIBUTE_021=strip(ATTRIBUTE_021);
  ATTRIBUTE_022=strip(ATTRIBUTE_022);
  ATTRIBUTE_023=strip(ATTRIBUTE_023);
  ATTRIBUTE_024=strip(ATTRIBUTE_024);
  ATTRIBUTE_025=strip(ATTRIBUTE_025);
  ATTRIBUTE_026=strip(ATTRIBUTE_026);
  ATTRIBUTE_027=strip(ATTRIBUTE_027);
  ATTRIBUTE_028=strip(ATTRIBUTE_028);
  ATTRIBUTE_029=strip(ATTRIBUTE_029);
  ATTRIBUTE_030=strip(ATTRIBUTE_030);
  ATTRIBUTE_031=strip(ATTRIBUTE_031);
  ATTRIBUTE_032=strip(ATTRIBUTE_032);
  ATTRIBUTE_033=strip(ATTRIBUTE_033);
  ATTRIBUTE_034=strip(ATTRIBUTE_034);
  ATTRIBUTE_035=strip(ATTRIBUTE_035);
  ATTRIBUTE_036=strip(ATTRIBUTE_036);
  ATTRIBUTE_037=strip(ATTRIBUTE_037);
  ATTRIBUTE_038=strip(ATTRIBUTE_038);
  ATTRIBUTE_039=strip(ATTRIBUTE_039);
  ATTRIBUTE_040=strip(ATTRIBUTE_040);
  ATTRIBUTE_041=strip(ATTRIBUTE_041);
  ATTRIBUTE_042=strip(ATTRIBUTE_042);
  ATTRIBUTE_043=strip(ATTRIBUTE_043);
  ATTRIBUTE_044=strip(ATTRIBUTE_044);
  ATTRIBUTE_045=strip(ATTRIBUTE_045);
  ATTRIBUTE_046=strip(ATTRIBUTE_046);
  ATTRIBUTE_047=strip(ATTRIBUTE_047);
  ATTRIBUTE_048=strip(ATTRIBUTE_048);
  ATTRIBUTE_049=strip(ATTRIBUTE_049);
  ATTRIBUTE_050=strip(ATTRIBUTE_050);
  ATTRIBUTE_051=strip(ATTRIBUTE_051);
  ATTRIBUTE_052=strip(ATTRIBUTE_052);
  ATTRIBUTE_053=strip(ATTRIBUTE_053);
  ATTRIBUTE_054=strip(ATTRIBUTE_054);
  ATTRIBUTE_055=strip(ATTRIBUTE_055);
  ATTRIBUTE_056=strip(ATTRIBUTE_056);
  ATTRIBUTE_057=strip(ATTRIBUTE_057);
  ATTRIBUTE_058=strip(ATTRIBUTE_058);
  ATTRIBUTE_059=strip(ATTRIBUTE_059);
  ATTRIBUTE_060=strip(ATTRIBUTE_060);
  ATTRIBUTE_061=strip(ATTRIBUTE_061);
  ATTRIBUTE_062=strip(ATTRIBUTE_062);
  ATTRIBUTE_063=strip(ATTRIBUTE_063);
  ATTRIBUTE_064=strip(ATTRIBUTE_064);
  ATTRIBUTE_065=strip(ATTRIBUTE_065);
  ATTRIBUTE_066=strip(ATTRIBUTE_066);
  ATTRIBUTE_067=strip(ATTRIBUTE_067);
  ATTRIBUTE_068=strip(ATTRIBUTE_068);
  ATTRIBUTE_069=strip(ATTRIBUTE_069);
  ATTRIBUTE_070=strip(ATTRIBUTE_070);
  ATTRIBUTE_071=strip(ATTRIBUTE_071);
  ATTRIBUTE_072=strip(ATTRIBUTE_072);
  ATTRIBUTE_073=strip(ATTRIBUTE_073);
  ATTRIBUTE_074=strip(ATTRIBUTE_074);
  ATTRIBUTE_075=strip(ATTRIBUTE_075);
  ATTRIBUTE_076=strip(ATTRIBUTE_076);
  ATTRIBUTE_077=strip(ATTRIBUTE_077);
  ATTRIBUTE_078=strip(ATTRIBUTE_078);
  ATTRIBUTE_079=strip(ATTRIBUTE_079);
  ATTRIBUTE_080=strip(ATTRIBUTE_080);
  ATTRIBUTE_081=strip(ATTRIBUTE_081);
  ATTRIBUTE_082=strip(ATTRIBUTE_082);
  ATTRIBUTE_083=strip(ATTRIBUTE_083);
  ATTRIBUTE_084=strip(ATTRIBUTE_084);
  ATTRIBUTE_085=strip(ATTRIBUTE_085);
  ATTRIBUTE_086=strip(ATTRIBUTE_086);
  ATTRIBUTE_087=strip(ATTRIBUTE_087);
  ATTRIBUTE_088=strip(ATTRIBUTE_088);
  ATTRIBUTE_089=strip(ATTRIBUTE_089);
  ATTRIBUTE_090=strip(ATTRIBUTE_090);
  if missing(METRIC_001) then METRIC_001=.;
  if missing(METRIC_002) then METRIC_002=.;
  if missing(METRIC_003) then METRIC_003=.;
  if missing(METRIC_004) then METRIC_004=.;
  if missing(METRIC_005) then METRIC_005=.;
  if missing(METRIC_006) then METRIC_006=.;
  if missing(METRIC_007) then METRIC_007=.;
  if missing(METRIC_008) then METRIC_008=.;
  if missing(METRIC_009) then METRIC_009=.;
  if missing(METRIC_010) then METRIC_010=.;
  if missing(METRIC_011) then METRIC_011=.;
  if missing(METRIC_012) then METRIC_012=.;
  if missing(METRIC_013) then METRIC_013=.;
  if missing(METRIC_014) then METRIC_014=.;
  if missing(METRIC_015) then METRIC_015=.;
  if missing(METRIC_016) then METRIC_016=.;
  if missing(METRIC_017) then METRIC_017=.;
  if missing(METRIC_018) then METRIC_018=.;
  if missing(METRIC_019) then METRIC_019=.;
  if missing(METRIC_020) then METRIC_020=.;
  if missing(METRIC_021) then METRIC_021=.;
  if missing(METRIC_022) then METRIC_022=.;
  if missing(METRIC_023) then METRIC_023=.;
  if missing(METRIC_024) then METRIC_024=.;
  if missing(METRIC_025) then METRIC_025=.;
  if missing(METRIC_026) then METRIC_026=.;
  if missing(METRIC_027) then METRIC_027=.;
  if missing(METRIC_028) then METRIC_028=.;
  if missing(METRIC_029) then METRIC_029=.;
  if missing(METRIC_030) then METRIC_030=.;
  if missing(METRIC_031) then METRIC_031=.;
  if missing(METRIC_032) then METRIC_032=.;
  if missing(METRIC_033) then METRIC_033=.;
  if missing(METRIC_034) then METRIC_034=.;
  if missing(METRIC_035) then METRIC_035=.;
  if missing(METRIC_036) then METRIC_036=.;
  if missing(METRIC_037) then METRIC_037=.;
  if missing(METRIC_038) then METRIC_038=.;
  if missing(METRIC_039) then METRIC_039=.;
  if missing(METRIC_040) then METRIC_040=.;
  if missing(METRIC_041) then METRIC_041=.;
  if missing(METRIC_042) then METRIC_042=.;
  if missing(METRIC_043) then METRIC_043=.;
  if missing(METRIC_044) then METRIC_044=.;
  if missing(METRIC_045) then METRIC_045=.;
  if missing(METRIC_046) then METRIC_046=.;
  if missing(METRIC_047) then METRIC_047=.;
  if missing(METRIC_048) then METRIC_048=.;
  if missing(METRIC_049) then METRIC_049=.;
  if missing(METRIC_050) then METRIC_050=.;
  if missing(METRIC_051) then METRIC_051=.;
  if missing(METRIC_052) then METRIC_052=.;
  if missing(METRIC_053) then METRIC_053=.;
  if missing(METRIC_054) then METRIC_054=.;
  if missing(METRIC_055) then METRIC_055=.;
  if missing(METRIC_056) then METRIC_056=.;
  if missing(METRIC_057) then METRIC_057=.;
  if missing(METRIC_058) then METRIC_058=.;
  if missing(METRIC_059) then METRIC_059=.;
  if missing(METRIC_060) then METRIC_060=.;
run;

%log_step(BUSINESS_VALIDATION);
data stage.transactions_validated stage.transactions_rejected;
  set stage.transactions_normalized;
  length rejection_reason $1000;
  rejection_reason='';
  if missing(customer_id) then rejection_reason=catx('|',rejection_reason,'CUSTOMER_REQUIRED');
  if missing(account_id) then rejection_reason=catx('|',rejection_reason,'ACCOUNT_REQUIRED');
  if missing(transaction_id) then rejection_reason=catx('|',rejection_reason,'TRANSACTION_REQUIRED');
  if missing(transaction_date) then rejection_reason=catx('|',rejection_reason,'DATE_REQUIRED');
  if missing(amount) then rejection_reason=catx('|',rejection_reason,'AMOUNT_REQUIRED');
  if amount < 0 and reversal_flag ne 'Y' then rejection_reason=catx('|',rejection_reason,'NEGATIVE_AMOUNT_NOT_REVERSAL');
  if amount > 10000000 then rejection_reason=catx('|',rejection_reason,'AMOUNT_LIMIT_EXCEEDED');
  if transaction_date > today() then rejection_reason=catx('|',rejection_reason,'FUTURE_TRANSACTION_DATE');
  if posting_date < transaction_date then rejection_reason=catx('|',rejection_reason,'POSTING_BEFORE_TRANSACTION');
  if length(currency) ne 3 then rejection_reason=catx('|',rejection_reason,'INVALID_CURRENCY_LENGTH');
  if length(country_code) ne 2 then rejection_reason=catx('|',rejection_reason,'INVALID_COUNTRY_CODE_LENGTH');
  if status not in ('PENDING','POSTED','SETTLED','REVERSED','FAILED') then rejection_reason=catx('|',rejection_reason,'INVALID_STATUS');
  if reversal_flag='INVALID' then rejection_reason=catx('|',rejection_reason,'INVALID_REVERSAL_FLAG');
  if rejection_reason='' then output stage.transactions_validated;
  else output stage.transactions_rejected;
run;

%log_step(ENRICH_TRANSACTIONS);
data stage.transactions_enriched;
  set stage.transactions_validated;
  length amount_band $20 transaction_month $7;
  if amount < 100 then amount_band='LOW';
  else if amount < 1000 then amount_band='MEDIUM';
  else if amount < 10000 then amount_band='HIGH';
  else amount_band='VERY_HIGH';
  transaction_month=put(transaction_date,yymmn7.);
  transaction_year=year(transaction_date);
  transaction_quarter=qtr(transaction_date);
  transaction_day=day(transaction_date);
  transaction_weekday=weekday(transaction_date);
  transaction_week=week(transaction_date,'U');
  is_weekend=(transaction_weekday in (1,7));
  gross_amount=round(amount+fee_amount+tax_amount,.01);
  calculated_net_amount=round(amount-fee_amount-tax_amount,.01);
  amount_difference=round(net_amount-calculated_net_amount,.01);
  if abs(amount_difference) <= &AMOUNT_TOLERANCE then amount_check='PASS';
  else amount_check='FAIL';
  if status='REVERSED' or reversal_flag='Y' then is_reversal=1;
  else is_reversal=0;
run;

%log_step(DEDUPLICATION);
proc sort data=stage.transactions_enriched out=stage.transactions_sorted;
  by transaction_id descending load_timestamp;
run;
data stage.transactions_deduplicated stage.transactions_duplicates;
  set stage.transactions_sorted;
  by transaction_id;
  if first.transaction_id then output stage.transactions_deduplicated;
  else output stage.transactions_duplicates;
run;

%log_step(CURRENCY_REFERENCE_JOIN);
proc sql;
  create table stage.transactions_with_currency as
  select a.*, b.currency_name, b.minor_units, b.is_active as currency_active
  from stage.transactions_deduplicated as a
  left join raw.currency_reference as b
    on a.currency=b.currency_code;
quit;

%log_step(CUSTOMER_REFERENCE_JOIN);
proc sql;
  create table stage.transactions_with_customer as
  select a.*, b.customer_segment, b.customer_status, b.risk_category
  from stage.transactions_with_currency as a
  left join raw.customer_reference as b
    on a.customer_id=b.customer_id;
quit;

%log_step(ACCOUNT_REFERENCE_JOIN);
proc sql;
  create table stage.transactions_final_stage as
  select a.*, b.account_type, b.account_status, b.account_open_date
  from stage.transactions_with_customer as a
  left join raw.account_reference as b
    on a.account_id=b.account_id;
quit;

%log_step(AGGREGATE_CUSTOMER);
proc sql;
  create table mart.customer_transaction_summary as
  select customer_id, currency, transaction_month,
         count(*) as transaction_count,
         sum(amount) as total_amount format=comma20.2,
         sum(fee_amount) as total_fees format=comma20.2,
         sum(tax_amount) as total_tax format=comma20.2,
         sum(calculated_net_amount) as total_net format=comma20.2,
         sum(is_reversal) as reversal_count,
         mean(amount) as average_amount format=comma20.2
  from stage.transactions_final_stage
  group by customer_id, currency, transaction_month;
quit;

%log_step(AGGREGATE_COUNTRY);
proc sql;
  create table mart.country_transaction_summary as
  select country_code, currency, transaction_month,
         count(*) as transaction_count,
         sum(amount) as total_amount,
         sum(calculated_net_amount) as total_net,
         count(distinct customer_id) as unique_customers
  from stage.transactions_final_stage
  group by country_code, currency, transaction_month;
quit;

%log_step(AGGREGATE_CHANNEL);
proc sql;
  create table mart.channel_transaction_summary as
  select channel, status, transaction_month,
         count(*) as transaction_count,
         sum(amount) as total_amount,
         mean(amount) as average_amount
  from stage.transactions_final_stage
  group by channel, status, transaction_month;
quit;

%log_step(DATA_QUALITY_METRICS);
proc sql;
  create table mart.data_quality_metrics as
  select count(*) as total_rows,
         sum(case when missing(customer_id) then 1 else 0 end) as missing_customer_id,
         sum(case when missing(account_id) then 1 else 0 end) as missing_account_id,
         sum(case when missing(transaction_id) then 1 else 0 end) as missing_transaction_id,
         sum(case when missing(transaction_date) then 1 else 0 end) as missing_transaction_date,
         sum(case when missing(amount) then 1 else 0 end) as missing_amount,
         sum(case when amount_check='FAIL' then 1 else 0 end) as amount_mismatch_count
  from stage.transactions_final_stage;
quit;

%log_step(WRITE_OUTPUTS);
data mart.transactions_final;
  set stage.transactions_final_stage;
run;

proc export data=mart.transactions_final
  outfile="&OUTPUT_PATH./transactions_final.csv"
  dbms=csv replace;
run;

proc export data=stage.transactions_rejected
  outfile="&OUTPUT_PATH./transactions_rejected.csv"
  dbms=csv replace;
run;

%count_rows(ds=mart.transactions_final,outvar=FINAL_COUNT);
%count_rows(ds=stage.transactions_rejected,outvar=REJECTED_COUNT);
%count_rows(ds=stage.transactions_duplicates,outvar=DUPLICATE_COUNT);

%put NOTE: ETL COMPLETED.;
%put NOTE: RAW=&RAW_COUNT FINAL=&FINAL_COUNT REJECTED=&REJECTED_COUNT DUPLICATES=&DUPLICATE_COUNT;

/* Field-level validation and audit rules follow. */
/* Required-field audit rule: CUSTOMER_ID */
data _null_; set stage.transactions_final_stage;
  if missing(CUSTOMER_ID) then put 'WARN: required field CUSTOMER_ID is missing for transaction=' transaction_id;
run;
/* String whitespace audit rule: CUSTOMER_ID */
data _null_; set stage.transactions_final_stage;
  if CUSTOMER_ID ne strip(CUSTOMER_ID) then put 'WARN: surrounding whitespace in CUSTOMER_ID';
run;
/* Required-field audit rule: ACCOUNT_ID */
data _null_; set stage.transactions_final_stage;
  if missing(ACCOUNT_ID) then put 'WARN: required field ACCOUNT_ID is missing for transaction=' transaction_id;
run;
/* String whitespace audit rule: ACCOUNT_ID */
data _null_; set stage.transactions_final_stage;
  if ACCOUNT_ID ne strip(ACCOUNT_ID) then put 'WARN: surrounding whitespace in ACCOUNT_ID';
run;
/* Required-field audit rule: TRANSACTION_ID */
data _null_; set stage.transactions_final_stage;
  if missing(TRANSACTION_ID) then put 'WARN: required field TRANSACTION_ID is missing for transaction=' transaction_id;
run;
/* String whitespace audit rule: TRANSACTION_ID */
data _null_; set stage.transactions_final_stage;
  if TRANSACTION_ID ne strip(TRANSACTION_ID) then put 'WARN: surrounding whitespace in TRANSACTION_ID';
run;
/* Required-field audit rule: TRANSACTION_DATE */
data _null_; set stage.transactions_final_stage;
  if missing(TRANSACTION_DATE) then put 'WARN: required field TRANSACTION_DATE is missing for transaction=' transaction_id;
run;
/* Required-field audit rule: AMOUNT */
data _null_; set stage.transactions_final_stage;
  if missing(AMOUNT) then put 'WARN: required field AMOUNT is missing for transaction=' transaction_id;
run;
/* Numeric range audit rule: AMOUNT */
data _null_; set stage.transactions_final_stage;
  if not missing(AMOUNT) and abs(AMOUNT) > 999999999999 then put 'WARN: out-of-range AMOUNT';
run;
/* Required-field audit rule: CURRENCY */
data _null_; set stage.transactions_final_stage;
  if missing(CURRENCY) then put 'WARN: required field CURRENCY is missing for transaction=' transaction_id;
run;
/* String whitespace audit rule: CURRENCY */
data _null_; set stage.transactions_final_stage;
  if CURRENCY ne strip(CURRENCY) then put 'WARN: surrounding whitespace in CURRENCY';
run;
/* Required-field audit rule: COUNTRY_CODE */
data _null_; set stage.transactions_final_stage;
  if missing(COUNTRY_CODE) then put 'WARN: required field COUNTRY_CODE is missing for transaction=' transaction_id;
run;
/* String whitespace audit rule: COUNTRY_CODE */
data _null_; set stage.transactions_final_stage;
  if COUNTRY_CODE ne strip(COUNTRY_CODE) then put 'WARN: surrounding whitespace in COUNTRY_CODE';
run;
/* String whitespace audit rule: CHANNEL */
data _null_; set stage.transactions_final_stage;
  if CHANNEL ne strip(CHANNEL) then put 'WARN: surrounding whitespace in CHANNEL';
run;
/* String whitespace audit rule: PRODUCT_CODE */
data _null_; set stage.transactions_final_stage;
  if PRODUCT_CODE ne strip(PRODUCT_CODE) then put 'WARN: surrounding whitespace in PRODUCT_CODE';
run;
/* Required-field audit rule: STATUS */
data _null_; set stage.transactions_final_stage;
  if missing(STATUS) then put 'WARN: required field STATUS is missing for transaction=' transaction_id;
run;
/* String whitespace audit rule: STATUS */
data _null_; set stage.transactions_final_stage;
  if STATUS ne strip(STATUS) then put 'WARN: surrounding whitespace in STATUS';
run;
/* Numeric range audit rule: FEE_AMOUNT */
data _null_; set stage.transactions_final_stage;
  if not missing(FEE_AMOUNT) and abs(FEE_AMOUNT) > 999999999999 then put 'WARN: out-of-range FEE_AMOUNT';
run;
/* Numeric range audit rule: TAX_AMOUNT */
data _null_; set stage.transactions_final_stage;
  if not missing(TAX_AMOUNT) and abs(TAX_AMOUNT) > 999999999999 then put 'WARN: out-of-range TAX_AMOUNT';
run;
/* Numeric range audit rule: NET_AMOUNT */
data _null_; set stage.transactions_final_stage;
  if not missing(NET_AMOUNT) and abs(NET_AMOUNT) > 999999999999 then put 'WARN: out-of-range NET_AMOUNT';
run;
/* Required-field audit rule: SOURCE_SYSTEM */
data _null_; set stage.transactions_final_stage;
  if missing(SOURCE_SYSTEM) then put 'WARN: required field SOURCE_SYSTEM is missing for transaction=' transaction_id;
run;
/* String whitespace audit rule: SOURCE_SYSTEM */
data _null_; set stage.transactions_final_stage;
  if SOURCE_SYSTEM ne strip(SOURCE_SYSTEM) then put 'WARN: surrounding whitespace in SOURCE_SYSTEM';
run;
/* Required-field audit rule: BATCH_ID */
data _null_; set stage.transactions_final_stage;
  if missing(BATCH_ID) then put 'WARN: required field BATCH_ID is missing for transaction=' transaction_id;
run;
/* String whitespace audit rule: BATCH_ID */
data _null_; set stage.transactions_final_stage;
  if BATCH_ID ne strip(BATCH_ID) then put 'WARN: surrounding whitespace in BATCH_ID';
run;
/* String whitespace audit rule: ATTRIBUTE_001 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_001 ne strip(ATTRIBUTE_001) then put 'WARN: surrounding whitespace in ATTRIBUTE_001';
run;
/* String whitespace audit rule: ATTRIBUTE_002 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_002 ne strip(ATTRIBUTE_002) then put 'WARN: surrounding whitespace in ATTRIBUTE_002';
run;
/* String whitespace audit rule: ATTRIBUTE_003 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_003 ne strip(ATTRIBUTE_003) then put 'WARN: surrounding whitespace in ATTRIBUTE_003';
run;
/* String whitespace audit rule: ATTRIBUTE_004 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_004 ne strip(ATTRIBUTE_004) then put 'WARN: surrounding whitespace in ATTRIBUTE_004';
run;
/* String whitespace audit rule: ATTRIBUTE_005 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_005 ne strip(ATTRIBUTE_005) then put 'WARN: surrounding whitespace in ATTRIBUTE_005';
run;
/* String whitespace audit rule: ATTRIBUTE_006 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_006 ne strip(ATTRIBUTE_006) then put 'WARN: surrounding whitespace in ATTRIBUTE_006';
run;
/* String whitespace audit rule: ATTRIBUTE_007 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_007 ne strip(ATTRIBUTE_007) then put 'WARN: surrounding whitespace in ATTRIBUTE_007';
run;
/* String whitespace audit rule: ATTRIBUTE_008 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_008 ne strip(ATTRIBUTE_008) then put 'WARN: surrounding whitespace in ATTRIBUTE_008';
run;
/* String whitespace audit rule: ATTRIBUTE_009 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_009 ne strip(ATTRIBUTE_009) then put 'WARN: surrounding whitespace in ATTRIBUTE_009';
run;
/* String whitespace audit rule: ATTRIBUTE_010 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_010 ne strip(ATTRIBUTE_010) then put 'WARN: surrounding whitespace in ATTRIBUTE_010';
run;
/* String whitespace audit rule: ATTRIBUTE_011 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_011 ne strip(ATTRIBUTE_011) then put 'WARN: surrounding whitespace in ATTRIBUTE_011';
run;
/* String whitespace audit rule: ATTRIBUTE_012 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_012 ne strip(ATTRIBUTE_012) then put 'WARN: surrounding whitespace in ATTRIBUTE_012';
run;
/* String whitespace audit rule: ATTRIBUTE_013 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_013 ne strip(ATTRIBUTE_013) then put 'WARN: surrounding whitespace in ATTRIBUTE_013';
run;
/* String whitespace audit rule: ATTRIBUTE_014 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_014 ne strip(ATTRIBUTE_014) then put 'WARN: surrounding whitespace in ATTRIBUTE_014';
run;
/* String whitespace audit rule: ATTRIBUTE_015 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_015 ne strip(ATTRIBUTE_015) then put 'WARN: surrounding whitespace in ATTRIBUTE_015';
run;
/* String whitespace audit rule: ATTRIBUTE_016 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_016 ne strip(ATTRIBUTE_016) then put 'WARN: surrounding whitespace in ATTRIBUTE_016';
run;
/* String whitespace audit rule: ATTRIBUTE_017 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_017 ne strip(ATTRIBUTE_017) then put 'WARN: surrounding whitespace in ATTRIBUTE_017';
run;
/* String whitespace audit rule: ATTRIBUTE_018 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_018 ne strip(ATTRIBUTE_018) then put 'WARN: surrounding whitespace in ATTRIBUTE_018';
run;
/* String whitespace audit rule: ATTRIBUTE_019 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_019 ne strip(ATTRIBUTE_019) then put 'WARN: surrounding whitespace in ATTRIBUTE_019';
run;
/* String whitespace audit rule: ATTRIBUTE_020 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_020 ne strip(ATTRIBUTE_020) then put 'WARN: surrounding whitespace in ATTRIBUTE_020';
run;
/* String whitespace audit rule: ATTRIBUTE_021 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_021 ne strip(ATTRIBUTE_021) then put 'WARN: surrounding whitespace in ATTRIBUTE_021';
run;
/* String whitespace audit rule: ATTRIBUTE_022 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_022 ne strip(ATTRIBUTE_022) then put 'WARN: surrounding whitespace in ATTRIBUTE_022';
run;
/* String whitespace audit rule: ATTRIBUTE_023 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_023 ne strip(ATTRIBUTE_023) then put 'WARN: surrounding whitespace in ATTRIBUTE_023';
run;
/* String whitespace audit rule: ATTRIBUTE_024 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_024 ne strip(ATTRIBUTE_024) then put 'WARN: surrounding whitespace in ATTRIBUTE_024';
run;
/* String whitespace audit rule: ATTRIBUTE_025 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_025 ne strip(ATTRIBUTE_025) then put 'WARN: surrounding whitespace in ATTRIBUTE_025';
run;
/* String whitespace audit rule: ATTRIBUTE_026 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_026 ne strip(ATTRIBUTE_026) then put 'WARN: surrounding whitespace in ATTRIBUTE_026';
run;
/* String whitespace audit rule: ATTRIBUTE_027 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_027 ne strip(ATTRIBUTE_027) then put 'WARN: surrounding whitespace in ATTRIBUTE_027';
run;
/* String whitespace audit rule: ATTRIBUTE_028 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_028 ne strip(ATTRIBUTE_028) then put 'WARN: surrounding whitespace in ATTRIBUTE_028';
run;
/* String whitespace audit rule: ATTRIBUTE_029 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_029 ne strip(ATTRIBUTE_029) then put 'WARN: surrounding whitespace in ATTRIBUTE_029';
run;
/* String whitespace audit rule: ATTRIBUTE_030 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_030 ne strip(ATTRIBUTE_030) then put 'WARN: surrounding whitespace in ATTRIBUTE_030';
run;
/* String whitespace audit rule: ATTRIBUTE_031 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_031 ne strip(ATTRIBUTE_031) then put 'WARN: surrounding whitespace in ATTRIBUTE_031';
run;
/* String whitespace audit rule: ATTRIBUTE_032 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_032 ne strip(ATTRIBUTE_032) then put 'WARN: surrounding whitespace in ATTRIBUTE_032';
run;
/* String whitespace audit rule: ATTRIBUTE_033 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_033 ne strip(ATTRIBUTE_033) then put 'WARN: surrounding whitespace in ATTRIBUTE_033';
run;
/* String whitespace audit rule: ATTRIBUTE_034 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_034 ne strip(ATTRIBUTE_034) then put 'WARN: surrounding whitespace in ATTRIBUTE_034';
run;
/* String whitespace audit rule: ATTRIBUTE_035 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_035 ne strip(ATTRIBUTE_035) then put 'WARN: surrounding whitespace in ATTRIBUTE_035';
run;
/* String whitespace audit rule: ATTRIBUTE_036 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_036 ne strip(ATTRIBUTE_036) then put 'WARN: surrounding whitespace in ATTRIBUTE_036';
run;
/* String whitespace audit rule: ATTRIBUTE_037 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_037 ne strip(ATTRIBUTE_037) then put 'WARN: surrounding whitespace in ATTRIBUTE_037';
run;
/* String whitespace audit rule: ATTRIBUTE_038 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_038 ne strip(ATTRIBUTE_038) then put 'WARN: surrounding whitespace in ATTRIBUTE_038';
run;
/* String whitespace audit rule: ATTRIBUTE_039 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_039 ne strip(ATTRIBUTE_039) then put 'WARN: surrounding whitespace in ATTRIBUTE_039';
run;
/* String whitespace audit rule: ATTRIBUTE_040 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_040 ne strip(ATTRIBUTE_040) then put 'WARN: surrounding whitespace in ATTRIBUTE_040';
run;
/* String whitespace audit rule: ATTRIBUTE_041 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_041 ne strip(ATTRIBUTE_041) then put 'WARN: surrounding whitespace in ATTRIBUTE_041';
run;
/* String whitespace audit rule: ATTRIBUTE_042 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_042 ne strip(ATTRIBUTE_042) then put 'WARN: surrounding whitespace in ATTRIBUTE_042';
run;
/* String whitespace audit rule: ATTRIBUTE_043 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_043 ne strip(ATTRIBUTE_043) then put 'WARN: surrounding whitespace in ATTRIBUTE_043';
run;
/* String whitespace audit rule: ATTRIBUTE_044 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_044 ne strip(ATTRIBUTE_044) then put 'WARN: surrounding whitespace in ATTRIBUTE_044';
run;
/* String whitespace audit rule: ATTRIBUTE_045 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_045 ne strip(ATTRIBUTE_045) then put 'WARN: surrounding whitespace in ATTRIBUTE_045';
run;
/* String whitespace audit rule: ATTRIBUTE_046 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_046 ne strip(ATTRIBUTE_046) then put 'WARN: surrounding whitespace in ATTRIBUTE_046';
run;
/* String whitespace audit rule: ATTRIBUTE_047 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_047 ne strip(ATTRIBUTE_047) then put 'WARN: surrounding whitespace in ATTRIBUTE_047';
run;
/* String whitespace audit rule: ATTRIBUTE_048 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_048 ne strip(ATTRIBUTE_048) then put 'WARN: surrounding whitespace in ATTRIBUTE_048';
run;
/* String whitespace audit rule: ATTRIBUTE_049 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_049 ne strip(ATTRIBUTE_049) then put 'WARN: surrounding whitespace in ATTRIBUTE_049';
run;
/* String whitespace audit rule: ATTRIBUTE_050 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_050 ne strip(ATTRIBUTE_050) then put 'WARN: surrounding whitespace in ATTRIBUTE_050';
run;
/* String whitespace audit rule: ATTRIBUTE_051 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_051 ne strip(ATTRIBUTE_051) then put 'WARN: surrounding whitespace in ATTRIBUTE_051';
run;
/* String whitespace audit rule: ATTRIBUTE_052 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_052 ne strip(ATTRIBUTE_052) then put 'WARN: surrounding whitespace in ATTRIBUTE_052';
run;
/* String whitespace audit rule: ATTRIBUTE_053 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_053 ne strip(ATTRIBUTE_053) then put 'WARN: surrounding whitespace in ATTRIBUTE_053';
run;
/* String whitespace audit rule: ATTRIBUTE_054 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_054 ne strip(ATTRIBUTE_054) then put 'WARN: surrounding whitespace in ATTRIBUTE_054';
run;
/* String whitespace audit rule: ATTRIBUTE_055 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_055 ne strip(ATTRIBUTE_055) then put 'WARN: surrounding whitespace in ATTRIBUTE_055';
run;
/* String whitespace audit rule: ATTRIBUTE_056 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_056 ne strip(ATTRIBUTE_056) then put 'WARN: surrounding whitespace in ATTRIBUTE_056';
run;
/* String whitespace audit rule: ATTRIBUTE_057 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_057 ne strip(ATTRIBUTE_057) then put 'WARN: surrounding whitespace in ATTRIBUTE_057';
run;
/* String whitespace audit rule: ATTRIBUTE_058 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_058 ne strip(ATTRIBUTE_058) then put 'WARN: surrounding whitespace in ATTRIBUTE_058';
run;
/* String whitespace audit rule: ATTRIBUTE_059 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_059 ne strip(ATTRIBUTE_059) then put 'WARN: surrounding whitespace in ATTRIBUTE_059';
run;
/* String whitespace audit rule: ATTRIBUTE_060 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_060 ne strip(ATTRIBUTE_060) then put 'WARN: surrounding whitespace in ATTRIBUTE_060';
run;
/* String whitespace audit rule: ATTRIBUTE_061 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_061 ne strip(ATTRIBUTE_061) then put 'WARN: surrounding whitespace in ATTRIBUTE_061';
run;
/* String whitespace audit rule: ATTRIBUTE_062 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_062 ne strip(ATTRIBUTE_062) then put 'WARN: surrounding whitespace in ATTRIBUTE_062';
run;
/* String whitespace audit rule: ATTRIBUTE_063 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_063 ne strip(ATTRIBUTE_063) then put 'WARN: surrounding whitespace in ATTRIBUTE_063';
run;
/* String whitespace audit rule: ATTRIBUTE_064 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_064 ne strip(ATTRIBUTE_064) then put 'WARN: surrounding whitespace in ATTRIBUTE_064';
run;
/* String whitespace audit rule: ATTRIBUTE_065 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_065 ne strip(ATTRIBUTE_065) then put 'WARN: surrounding whitespace in ATTRIBUTE_065';
run;
/* String whitespace audit rule: ATTRIBUTE_066 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_066 ne strip(ATTRIBUTE_066) then put 'WARN: surrounding whitespace in ATTRIBUTE_066';
run;
/* String whitespace audit rule: ATTRIBUTE_067 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_067 ne strip(ATTRIBUTE_067) then put 'WARN: surrounding whitespace in ATTRIBUTE_067';
run;
/* String whitespace audit rule: ATTRIBUTE_068 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_068 ne strip(ATTRIBUTE_068) then put 'WARN: surrounding whitespace in ATTRIBUTE_068';
run;
/* String whitespace audit rule: ATTRIBUTE_069 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_069 ne strip(ATTRIBUTE_069) then put 'WARN: surrounding whitespace in ATTRIBUTE_069';
run;
/* String whitespace audit rule: ATTRIBUTE_070 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_070 ne strip(ATTRIBUTE_070) then put 'WARN: surrounding whitespace in ATTRIBUTE_070';
run;
/* String whitespace audit rule: ATTRIBUTE_071 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_071 ne strip(ATTRIBUTE_071) then put 'WARN: surrounding whitespace in ATTRIBUTE_071';
run;
/* String whitespace audit rule: ATTRIBUTE_072 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_072 ne strip(ATTRIBUTE_072) then put 'WARN: surrounding whitespace in ATTRIBUTE_072';
run;
/* String whitespace audit rule: ATTRIBUTE_073 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_073 ne strip(ATTRIBUTE_073) then put 'WARN: surrounding whitespace in ATTRIBUTE_073';
run;
/* String whitespace audit rule: ATTRIBUTE_074 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_074 ne strip(ATTRIBUTE_074) then put 'WARN: surrounding whitespace in ATTRIBUTE_074';
run;
/* String whitespace audit rule: ATTRIBUTE_075 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_075 ne strip(ATTRIBUTE_075) then put 'WARN: surrounding whitespace in ATTRIBUTE_075';
run;
/* String whitespace audit rule: ATTRIBUTE_076 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_076 ne strip(ATTRIBUTE_076) then put 'WARN: surrounding whitespace in ATTRIBUTE_076';
run;
/* String whitespace audit rule: ATTRIBUTE_077 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_077 ne strip(ATTRIBUTE_077) then put 'WARN: surrounding whitespace in ATTRIBUTE_077';
run;
/* String whitespace audit rule: ATTRIBUTE_078 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_078 ne strip(ATTRIBUTE_078) then put 'WARN: surrounding whitespace in ATTRIBUTE_078';
run;
/* String whitespace audit rule: ATTRIBUTE_079 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_079 ne strip(ATTRIBUTE_079) then put 'WARN: surrounding whitespace in ATTRIBUTE_079';
run;
/* String whitespace audit rule: ATTRIBUTE_080 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_080 ne strip(ATTRIBUTE_080) then put 'WARN: surrounding whitespace in ATTRIBUTE_080';
run;
/* String whitespace audit rule: ATTRIBUTE_081 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_081 ne strip(ATTRIBUTE_081) then put 'WARN: surrounding whitespace in ATTRIBUTE_081';
run;
/* String whitespace audit rule: ATTRIBUTE_082 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_082 ne strip(ATTRIBUTE_082) then put 'WARN: surrounding whitespace in ATTRIBUTE_082';
run;
/* String whitespace audit rule: ATTRIBUTE_083 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_083 ne strip(ATTRIBUTE_083) then put 'WARN: surrounding whitespace in ATTRIBUTE_083';
run;
/* String whitespace audit rule: ATTRIBUTE_084 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_084 ne strip(ATTRIBUTE_084) then put 'WARN: surrounding whitespace in ATTRIBUTE_084';
run;
/* String whitespace audit rule: ATTRIBUTE_085 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_085 ne strip(ATTRIBUTE_085) then put 'WARN: surrounding whitespace in ATTRIBUTE_085';
run;
/* String whitespace audit rule: ATTRIBUTE_086 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_086 ne strip(ATTRIBUTE_086) then put 'WARN: surrounding whitespace in ATTRIBUTE_086';
run;
/* String whitespace audit rule: ATTRIBUTE_087 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_087 ne strip(ATTRIBUTE_087) then put 'WARN: surrounding whitespace in ATTRIBUTE_087';
run;
/* String whitespace audit rule: ATTRIBUTE_088 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_088 ne strip(ATTRIBUTE_088) then put 'WARN: surrounding whitespace in ATTRIBUTE_088';
run;
/* String whitespace audit rule: ATTRIBUTE_089 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_089 ne strip(ATTRIBUTE_089) then put 'WARN: surrounding whitespace in ATTRIBUTE_089';
run;
/* String whitespace audit rule: ATTRIBUTE_090 */
data _null_; set stage.transactions_final_stage;
  if ATTRIBUTE_090 ne strip(ATTRIBUTE_090) then put 'WARN: surrounding whitespace in ATTRIBUTE_090';
run;
/* Numeric range audit rule: METRIC_001 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_001) and abs(METRIC_001) > 999999999999 then put 'WARN: out-of-range METRIC_001';
run;
/* Numeric range audit rule: METRIC_002 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_002) and abs(METRIC_002) > 999999999999 then put 'WARN: out-of-range METRIC_002';
run;
/* Numeric range audit rule: METRIC_003 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_003) and abs(METRIC_003) > 999999999999 then put 'WARN: out-of-range METRIC_003';
run;
/* Numeric range audit rule: METRIC_004 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_004) and abs(METRIC_004) > 999999999999 then put 'WARN: out-of-range METRIC_004';
run;
/* Numeric range audit rule: METRIC_005 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_005) and abs(METRIC_005) > 999999999999 then put 'WARN: out-of-range METRIC_005';
run;
/* Numeric range audit rule: METRIC_006 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_006) and abs(METRIC_006) > 999999999999 then put 'WARN: out-of-range METRIC_006';
run;
/* Numeric range audit rule: METRIC_007 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_007) and abs(METRIC_007) > 999999999999 then put 'WARN: out-of-range METRIC_007';
run;
/* Numeric range audit rule: METRIC_008 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_008) and abs(METRIC_008) > 999999999999 then put 'WARN: out-of-range METRIC_008';
run;
/* Numeric range audit rule: METRIC_009 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_009) and abs(METRIC_009) > 999999999999 then put 'WARN: out-of-range METRIC_009';
run;
/* Numeric range audit rule: METRIC_010 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_010) and abs(METRIC_010) > 999999999999 then put 'WARN: out-of-range METRIC_010';
run;
/* Numeric range audit rule: METRIC_011 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_011) and abs(METRIC_011) > 999999999999 then put 'WARN: out-of-range METRIC_011';
run;
/* Numeric range audit rule: METRIC_012 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_012) and abs(METRIC_012) > 999999999999 then put 'WARN: out-of-range METRIC_012';
run;
/* Numeric range audit rule: METRIC_013 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_013) and abs(METRIC_013) > 999999999999 then put 'WARN: out-of-range METRIC_013';
run;
/* Numeric range audit rule: METRIC_014 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_014) and abs(METRIC_014) > 999999999999 then put 'WARN: out-of-range METRIC_014';
run;
/* Numeric range audit rule: METRIC_015 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_015) and abs(METRIC_015) > 999999999999 then put 'WARN: out-of-range METRIC_015';
run;
/* Numeric range audit rule: METRIC_016 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_016) and abs(METRIC_016) > 999999999999 then put 'WARN: out-of-range METRIC_016';
run;
/* Numeric range audit rule: METRIC_017 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_017) and abs(METRIC_017) > 999999999999 then put 'WARN: out-of-range METRIC_017';
run;
/* Numeric range audit rule: METRIC_018 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_018) and abs(METRIC_018) > 999999999999 then put 'WARN: out-of-range METRIC_018';
run;
/* Numeric range audit rule: METRIC_019 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_019) and abs(METRIC_019) > 999999999999 then put 'WARN: out-of-range METRIC_019';
run;
/* Numeric range audit rule: METRIC_020 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_020) and abs(METRIC_020) > 999999999999 then put 'WARN: out-of-range METRIC_020';
run;
/* Numeric range audit rule: METRIC_021 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_021) and abs(METRIC_021) > 999999999999 then put 'WARN: out-of-range METRIC_021';
run;
/* Numeric range audit rule: METRIC_022 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_022) and abs(METRIC_022) > 999999999999 then put 'WARN: out-of-range METRIC_022';
run;
/* Numeric range audit rule: METRIC_023 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_023) and abs(METRIC_023) > 999999999999 then put 'WARN: out-of-range METRIC_023';
run;
/* Numeric range audit rule: METRIC_024 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_024) and abs(METRIC_024) > 999999999999 then put 'WARN: out-of-range METRIC_024';
run;
/* Numeric range audit rule: METRIC_025 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_025) and abs(METRIC_025) > 999999999999 then put 'WARN: out-of-range METRIC_025';
run;
/* Numeric range audit rule: METRIC_026 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_026) and abs(METRIC_026) > 999999999999 then put 'WARN: out-of-range METRIC_026';
run;
/* Numeric range audit rule: METRIC_027 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_027) and abs(METRIC_027) > 999999999999 then put 'WARN: out-of-range METRIC_027';
run;
/* Numeric range audit rule: METRIC_028 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_028) and abs(METRIC_028) > 999999999999 then put 'WARN: out-of-range METRIC_028';
run;
/* Numeric range audit rule: METRIC_029 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_029) and abs(METRIC_029) > 999999999999 then put 'WARN: out-of-range METRIC_029';
run;
/* Numeric range audit rule: METRIC_030 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_030) and abs(METRIC_030) > 999999999999 then put 'WARN: out-of-range METRIC_030';
run;
/* Numeric range audit rule: METRIC_031 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_031) and abs(METRIC_031) > 999999999999 then put 'WARN: out-of-range METRIC_031';
run;
/* Numeric range audit rule: METRIC_032 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_032) and abs(METRIC_032) > 999999999999 then put 'WARN: out-of-range METRIC_032';
run;
/* Numeric range audit rule: METRIC_033 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_033) and abs(METRIC_033) > 999999999999 then put 'WARN: out-of-range METRIC_033';
run;
/* Numeric range audit rule: METRIC_034 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_034) and abs(METRIC_034) > 999999999999 then put 'WARN: out-of-range METRIC_034';
run;
/* Numeric range audit rule: METRIC_035 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_035) and abs(METRIC_035) > 999999999999 then put 'WARN: out-of-range METRIC_035';
run;
/* Numeric range audit rule: METRIC_036 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_036) and abs(METRIC_036) > 999999999999 then put 'WARN: out-of-range METRIC_036';
run;
/* Numeric range audit rule: METRIC_037 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_037) and abs(METRIC_037) > 999999999999 then put 'WARN: out-of-range METRIC_037';
run;
/* Numeric range audit rule: METRIC_038 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_038) and abs(METRIC_038) > 999999999999 then put 'WARN: out-of-range METRIC_038';
run;
/* Numeric range audit rule: METRIC_039 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_039) and abs(METRIC_039) > 999999999999 then put 'WARN: out-of-range METRIC_039';
run;
/* Numeric range audit rule: METRIC_040 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_040) and abs(METRIC_040) > 999999999999 then put 'WARN: out-of-range METRIC_040';
run;
/* Numeric range audit rule: METRIC_041 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_041) and abs(METRIC_041) > 999999999999 then put 'WARN: out-of-range METRIC_041';
run;
/* Numeric range audit rule: METRIC_042 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_042) and abs(METRIC_042) > 999999999999 then put 'WARN: out-of-range METRIC_042';
run;
/* Numeric range audit rule: METRIC_043 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_043) and abs(METRIC_043) > 999999999999 then put 'WARN: out-of-range METRIC_043';
run;
/* Numeric range audit rule: METRIC_044 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_044) and abs(METRIC_044) > 999999999999 then put 'WARN: out-of-range METRIC_044';
run;
/* Numeric range audit rule: METRIC_045 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_045) and abs(METRIC_045) > 999999999999 then put 'WARN: out-of-range METRIC_045';
run;
/* Numeric range audit rule: METRIC_046 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_046) and abs(METRIC_046) > 999999999999 then put 'WARN: out-of-range METRIC_046';
run;
/* Numeric range audit rule: METRIC_047 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_047) and abs(METRIC_047) > 999999999999 then put 'WARN: out-of-range METRIC_047';
run;
/* Numeric range audit rule: METRIC_048 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_048) and abs(METRIC_048) > 999999999999 then put 'WARN: out-of-range METRIC_048';
run;
/* Numeric range audit rule: METRIC_049 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_049) and abs(METRIC_049) > 999999999999 then put 'WARN: out-of-range METRIC_049';
run;
/* Numeric range audit rule: METRIC_050 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_050) and abs(METRIC_050) > 999999999999 then put 'WARN: out-of-range METRIC_050';
run;
/* Numeric range audit rule: METRIC_051 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_051) and abs(METRIC_051) > 999999999999 then put 'WARN: out-of-range METRIC_051';
run;
/* Numeric range audit rule: METRIC_052 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_052) and abs(METRIC_052) > 999999999999 then put 'WARN: out-of-range METRIC_052';
run;
/* Numeric range audit rule: METRIC_053 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_053) and abs(METRIC_053) > 999999999999 then put 'WARN: out-of-range METRIC_053';
run;
/* Numeric range audit rule: METRIC_054 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_054) and abs(METRIC_054) > 999999999999 then put 'WARN: out-of-range METRIC_054';
run;
/* Numeric range audit rule: METRIC_055 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_055) and abs(METRIC_055) > 999999999999 then put 'WARN: out-of-range METRIC_055';
run;
/* Numeric range audit rule: METRIC_056 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_056) and abs(METRIC_056) > 999999999999 then put 'WARN: out-of-range METRIC_056';
run;
/* Numeric range audit rule: METRIC_057 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_057) and abs(METRIC_057) > 999999999999 then put 'WARN: out-of-range METRIC_057';
run;
/* Numeric range audit rule: METRIC_058 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_058) and abs(METRIC_058) > 999999999999 then put 'WARN: out-of-range METRIC_058';
run;
/* Numeric range audit rule: METRIC_059 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_059) and abs(METRIC_059) > 999999999999 then put 'WARN: out-of-range METRIC_059';
run;
/* Numeric range audit rule: METRIC_060 */
data _null_; set stage.transactions_final_stage;
  if not missing(METRIC_060) and abs(METRIC_060) > 999999999999 then put 'WARN: out-of-range METRIC_060';
run;
/* Operational audit section 01: transaction cohort review */
data stage.audit_cohort_01;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 1, 5)=0;
  audit_rule_id='COHORT_01';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_01 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_01
  group by audit_rule_id;
quit;

/* Operational audit section 02: transaction cohort review */
data stage.audit_cohort_02;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 2, 5)=0;
  audit_rule_id='COHORT_02';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_02 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_02
  group by audit_rule_id;
quit;

/* Operational audit section 03: transaction cohort review */
data stage.audit_cohort_03;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 3, 5)=0;
  audit_rule_id='COHORT_03';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_03 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_03
  group by audit_rule_id;
quit;

/* Operational audit section 04: transaction cohort review */
data stage.audit_cohort_04;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 4, 5)=0;
  audit_rule_id='COHORT_04';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_04 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_04
  group by audit_rule_id;
quit;

/* Operational audit section 05: transaction cohort review */
data stage.audit_cohort_05;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 5, 5)=0;
  audit_rule_id='COHORT_05';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_05 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_05
  group by audit_rule_id;
quit;

/* Operational audit section 06: transaction cohort review */
data stage.audit_cohort_06;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 6, 5)=0;
  audit_rule_id='COHORT_06';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_06 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_06
  group by audit_rule_id;
quit;

/* Operational audit section 07: transaction cohort review */
data stage.audit_cohort_07;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 7, 5)=0;
  audit_rule_id='COHORT_07';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_07 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_07
  group by audit_rule_id;
quit;

/* Operational audit section 08: transaction cohort review */
data stage.audit_cohort_08;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 8, 5)=0;
  audit_rule_id='COHORT_08';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_08 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_08
  group by audit_rule_id;
quit;

/* Operational audit section 09: transaction cohort review */
data stage.audit_cohort_09;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 9, 5)=0;
  audit_rule_id='COHORT_09';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_09 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_09
  group by audit_rule_id;
quit;

/* Operational audit section 10: transaction cohort review */
data stage.audit_cohort_10;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 10, 5)=0;
  audit_rule_id='COHORT_10';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_10 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_10
  group by audit_rule_id;
quit;

/* Operational audit section 11: transaction cohort review */
data stage.audit_cohort_11;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 11, 5)=0;
  audit_rule_id='COHORT_11';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_11 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_11
  group by audit_rule_id;
quit;

/* Operational audit section 12: transaction cohort review */
data stage.audit_cohort_12;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 12, 5)=0;
  audit_rule_id='COHORT_12';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_12 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_12
  group by audit_rule_id;
quit;

/* Operational audit section 13: transaction cohort review */
data stage.audit_cohort_13;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 13, 5)=0;
  audit_rule_id='COHORT_13';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_13 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_13
  group by audit_rule_id;
quit;

/* Operational audit section 14: transaction cohort review */
data stage.audit_cohort_14;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 14, 5)=0;
  audit_rule_id='COHORT_14';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_14 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_14
  group by audit_rule_id;
quit;

/* Operational audit section 15: transaction cohort review */
data stage.audit_cohort_15;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 15, 5)=0;
  audit_rule_id='COHORT_15';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_15 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_15
  group by audit_rule_id;
quit;

/* Operational audit section 16: transaction cohort review */
data stage.audit_cohort_16;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 16, 5)=0;
  audit_rule_id='COHORT_16';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_16 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_16
  group by audit_rule_id;
quit;

/* Operational audit section 17: transaction cohort review */
data stage.audit_cohort_17;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 17, 5)=0;
  audit_rule_id='COHORT_17';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_17 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_17
  group by audit_rule_id;
quit;

/* Operational audit section 18: transaction cohort review */
data stage.audit_cohort_18;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 18, 5)=0;
  audit_rule_id='COHORT_18';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_18 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_18
  group by audit_rule_id;
quit;

/* Operational audit section 19: transaction cohort review */
data stage.audit_cohort_19;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 19, 5)=0;
  audit_rule_id='COHORT_19';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_19 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_19
  group by audit_rule_id;
quit;

/* Operational audit section 20: transaction cohort review */
data stage.audit_cohort_20;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 20, 5)=0;
  audit_rule_id='COHORT_20';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_20 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_20
  group by audit_rule_id;
quit;

/* Operational audit section 21: transaction cohort review */
data stage.audit_cohort_21;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 21, 5)=0;
  audit_rule_id='COHORT_21';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_21 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_21
  group by audit_rule_id;
quit;

/* Operational audit section 22: transaction cohort review */
data stage.audit_cohort_22;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 22, 5)=0;
  audit_rule_id='COHORT_22';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_22 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_22
  group by audit_rule_id;
quit;

/* Operational audit section 23: transaction cohort review */
data stage.audit_cohort_23;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 23, 5)=0;
  audit_rule_id='COHORT_23';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_23 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_23
  group by audit_rule_id;
quit;

/* Operational audit section 24: transaction cohort review */
data stage.audit_cohort_24;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 24, 5)=0;
  audit_rule_id='COHORT_24';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_24 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_24
  group by audit_rule_id;
quit;

/* Operational audit section 25: transaction cohort review */
data stage.audit_cohort_25;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 25, 5)=0;
  audit_rule_id='COHORT_25';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_25 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_25
  group by audit_rule_id;
quit;

/* Operational audit section 26: transaction cohort review */
data stage.audit_cohort_26;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 26, 5)=0;
  audit_rule_id='COHORT_26';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_26 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_26
  group by audit_rule_id;
quit;

/* Operational audit section 27: transaction cohort review */
data stage.audit_cohort_27;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 27, 5)=0;
  audit_rule_id='COHORT_27';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_27 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_27
  group by audit_rule_id;
quit;

/* Operational audit section 28: transaction cohort review */
data stage.audit_cohort_28;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 28, 5)=0;
  audit_rule_id='COHORT_28';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_28 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_28
  group by audit_rule_id;
quit;

/* Operational audit section 29: transaction cohort review */
data stage.audit_cohort_29;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 29, 5)=0;
  audit_rule_id='COHORT_29';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_29 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_29
  group by audit_rule_id;
quit;

/* Operational audit section 30: transaction cohort review */
data stage.audit_cohort_30;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 30, 5)=0;
  audit_rule_id='COHORT_30';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_30 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_30
  group by audit_rule_id;
quit;

/* Operational audit section 31: transaction cohort review */
data stage.audit_cohort_31;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 31, 5)=0;
  audit_rule_id='COHORT_31';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_31 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_31
  group by audit_rule_id;
quit;

/* Operational audit section 32: transaction cohort review */
data stage.audit_cohort_32;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 32, 5)=0;
  audit_rule_id='COHORT_32';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_32 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_32
  group by audit_rule_id;
quit;

/* Operational audit section 33: transaction cohort review */
data stage.audit_cohort_33;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 33, 5)=0;
  audit_rule_id='COHORT_33';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_33 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_33
  group by audit_rule_id;
quit;

/* Operational audit section 34: transaction cohort review */
data stage.audit_cohort_34;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 34, 5)=0;
  audit_rule_id='COHORT_34';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_34 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_34
  group by audit_rule_id;
quit;

/* Operational audit section 35: transaction cohort review */
data stage.audit_cohort_35;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 35, 5)=0;
  audit_rule_id='COHORT_35';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_35 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_35
  group by audit_rule_id;
quit;

/* Operational audit section 36: transaction cohort review */
data stage.audit_cohort_36;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 36, 5)=0;
  audit_rule_id='COHORT_36';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_36 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_36
  group by audit_rule_id;
quit;

/* Operational audit section 37: transaction cohort review */
data stage.audit_cohort_37;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 37, 5)=0;
  audit_rule_id='COHORT_37';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_37 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_37
  group by audit_rule_id;
quit;

/* Operational audit section 38: transaction cohort review */
data stage.audit_cohort_38;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 38, 5)=0;
  audit_rule_id='COHORT_38';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_38 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_38
  group by audit_rule_id;
quit;

/* Operational audit section 39: transaction cohort review */
data stage.audit_cohort_39;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 39, 5)=0;
  audit_rule_id='COHORT_39';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_39 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_39
  group by audit_rule_id;
quit;

/* Operational audit section 40: transaction cohort review */
data stage.audit_cohort_40;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 40, 5)=0;
  audit_rule_id='COHORT_40';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_40 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_40
  group by audit_rule_id;
quit;

/* Operational audit section 41: transaction cohort review */
data stage.audit_cohort_41;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 41, 5)=0;
  audit_rule_id='COHORT_41';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_41 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_41
  group by audit_rule_id;
quit;

/* Operational audit section 42: transaction cohort review */
data stage.audit_cohort_42;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 42, 5)=0;
  audit_rule_id='COHORT_42';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_42 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_42
  group by audit_rule_id;
quit;

/* Operational audit section 43: transaction cohort review */
data stage.audit_cohort_43;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 43, 5)=0;
  audit_rule_id='COHORT_43';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_43 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_43
  group by audit_rule_id;
quit;

/* Operational audit section 44: transaction cohort review */
data stage.audit_cohort_44;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 44, 5)=0;
  audit_rule_id='COHORT_44';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_44 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_44
  group by audit_rule_id;
quit;

/* Operational audit section 45: transaction cohort review */
data stage.audit_cohort_45;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 45, 5)=0;
  audit_rule_id='COHORT_45';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_45 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_45
  group by audit_rule_id;
quit;

/* Operational audit section 46: transaction cohort review */
data stage.audit_cohort_46;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 46, 5)=0;
  audit_rule_id='COHORT_46';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_46 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_46
  group by audit_rule_id;
quit;

/* Operational audit section 47: transaction cohort review */
data stage.audit_cohort_47;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 47, 5)=0;
  audit_rule_id='COHORT_47';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_47 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_47
  group by audit_rule_id;
quit;

/* Operational audit section 48: transaction cohort review */
data stage.audit_cohort_48;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 48, 5)=0;
  audit_rule_id='COHORT_48';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_48 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_48
  group by audit_rule_id;
quit;

/* Operational audit section 49: transaction cohort review */
data stage.audit_cohort_49;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 49, 5)=0;
  audit_rule_id='COHORT_49';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_49 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_49
  group by audit_rule_id;
quit;

/* Operational audit section 50: transaction cohort review */
data stage.audit_cohort_50;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 50, 5)=0;
  audit_rule_id='COHORT_50';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_50 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_50
  group by audit_rule_id;
quit;

/* Operational audit section 51: transaction cohort review */
data stage.audit_cohort_51;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 51, 5)=0;
  audit_rule_id='COHORT_51';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_51 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_51
  group by audit_rule_id;
quit;

/* Operational audit section 52: transaction cohort review */
data stage.audit_cohort_52;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 52, 5)=0;
  audit_rule_id='COHORT_52';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_52 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_52
  group by audit_rule_id;
quit;

/* Operational audit section 53: transaction cohort review */
data stage.audit_cohort_53;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 53, 5)=0;
  audit_rule_id='COHORT_53';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_53 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_53
  group by audit_rule_id;
quit;

/* Operational audit section 54: transaction cohort review */
data stage.audit_cohort_54;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 54, 5)=0;
  audit_rule_id='COHORT_54';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_54 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_54
  group by audit_rule_id;
quit;

/* Operational audit section 55: transaction cohort review */
data stage.audit_cohort_55;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 55, 5)=0;
  audit_rule_id='COHORT_55';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_55 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_55
  group by audit_rule_id;
quit;

/* Operational audit section 56: transaction cohort review */
data stage.audit_cohort_56;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 56, 5)=0;
  audit_rule_id='COHORT_56';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_56 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_56
  group by audit_rule_id;
quit;

/* Operational audit section 57: transaction cohort review */
data stage.audit_cohort_57;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 57, 5)=0;
  audit_rule_id='COHORT_57';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_57 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_57
  group by audit_rule_id;
quit;

/* Operational audit section 58: transaction cohort review */
data stage.audit_cohort_58;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 58, 5)=0;
  audit_rule_id='COHORT_58';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_58 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_58
  group by audit_rule_id;
quit;

/* Operational audit section 59: transaction cohort review */
data stage.audit_cohort_59;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 59, 5)=0;
  audit_rule_id='COHORT_59';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_59 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_59
  group by audit_rule_id;
quit;

/* Operational audit section 60: transaction cohort review */
data stage.audit_cohort_60;
  set stage.transactions_final_stage;
  if mod(transaction_year + transaction_quarter + 60, 5)=0;
  audit_rule_id='COHORT_60';
  audit_amount=round(amount,0.01);
  audit_date=transaction_date;
  audit_valid=(not missing(transaction_id) and not missing(audit_date));
run;
proc sql;
  create table mart.audit_summary_60 as
  select audit_rule_id, count(*) as rows_reviewed,
         sum(audit_valid) as rows_valid,
         sum(audit_amount) as amount_reviewed
  from stage.audit_cohort_60
  group by audit_rule_id;
quit;

/* Final integrity checks */
proc sql;
  create table mart.run_control as
  select &RAW_COUNT as raw_count,
         &FINAL_COUNT as final_count,
         &REJECTED_COUNT as rejected_count,
         &DUPLICATE_COUNT as duplicate_count,
         datetime() as completed_at format=e8601dt26.6;
quit;

