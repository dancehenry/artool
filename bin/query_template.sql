/*
======new features=========================================================================================
In this version, compared to that in the previous one archived on 01/31/2022, below are the new features.
1. We can load multiple files as the input of invoice sales csv files.

---0315---
1. Fixed a bug that previous month data was removed in the output spreadsheet.
===========================================================================================================
*/

CREATE SCHEMA IF NOT EXISTS stg;

SET search_path to stg;

DROP TABLE IF EXISTS stg.discount;
CREATE TABLE stg.discount (
    customer varchar(200) not null,
    discount decimal(3,2) not null
);

INSERT INTO stg.discount
SELECT '13287 Bed Bath & Beyond', 0.02 UNION ALL
SELECT '14615 Target (Dropship)', 0.02 UNION ALL
SELECT '13290 The Home Depot', 0.02;

/*
======input #1==============
output from previous cycle.
============================
*/

DROP TABLE IF EXISTS stg.output_from_previous_cycle;
CREATE TABLE stg.output_from_previous_cycle (
    customer varchar(200) null,
    invoice_date date null,
    po_number varchar(50) null,
    invoice_number varchar(50) null,
    sku varchar(100) null,
    quantity money null,
    unit_price money null,
    subtotal money null,
    invoice_total money null,
    discount_amount money null,
    payment_number varchar(50) null,
    payment_date date null,
    payment_received money null,
    ar_balance money null
);

copy stg.output_from_previous_cycle
from '~<PARAMETER_5>~'
delimiter ','
csv header;

/*
======input #2=========================
invoice sales report of current cycle.
=======================================
*/

DROP TABLE IF EXISTS stg.invoice_sales_input;
CREATE TABLE stg.invoice_sales_input (
    customer varchar(200) null,
    transaction_type varchar(50) null,
    date date null,
    invoice_number varchar(50) null,
    total_revenue money null,
    po_number varchar(50) null,
    quantity money null,
    unit_price money null,
    sku varchar(100) null
);

do $$
declare
files text[]:=array[~<PARAMETER_4>~];
copy_command text;
x text;
begin

drop table if exists stg.tmp_invoice_sales_input;
CREATE TABLE stg.tmp_invoice_sales_input (
    customer varchar(200) null,
    transaction_type varchar(50) null,
    date date null,
    invoice_number varchar(50) null,
    total_revenue money null,
    po_number varchar(50) null,
    quantity money null,
    unit_price money null,
    sku varchar(100) null
);

FOREACH x IN ARRAY files
  LOOP
    copy_command := 'copy stg.tmp_invoice_sales_input from '''|| x || '''delimiter '','' csv header';
    execute copy_command;

    insert into stg.invoice_sales_input
    select * from stg.tmp_invoice_sales_input;

    truncate stg.tmp_invoice_sales_input;
  END LOOP;
  drop table if exists stg.tmp_invoice_sales_input;
end;
$$;

DROP TABLE IF EXISTS stg.data_issue_invoice_sales;
CREATE TABLE stg.data_issue_invoice_sales AS
SELECT *
FROM stg.invoice_sales_input A
--WHERE NOT (A IS NOT NULL);
WHERE UPPER(BTRIM(NULLIF(transaction_type, 'INVOICE'))) != UPPER('INVOICE')
OR total_revenue is null
OR quantity is null
OR unit_price is null
OR BTRIM(NULLIF(sku, '')) = '';

DROP TABLE IF EXISTS stg.invoice_sales;
CREATE TABLE stg.invoice_sales AS
SELECT
    customer,
    transaction_type,
    date,
    invoice_number,
    total_revenue,
    po_number,
    CAST(quantity AS DECIMAL(9,2)) AS quantity,
    unit_price,
    sku
FROM stg.invoice_sales_input A
WHERE UPPER(BTRIM(NULLIF(transaction_type, 'INVOICE'))) = UPPER('INVOICE')
AND total_revenue is not null
AND quantity is not null
AND unit_price is not null
AND BTRIM(NULLIF(sku, '')) != '';

/*
======input #3=========================
customer payment of current cycle
=======================================
*/

DROP TABLE IF EXISTS stg.customer_payment_input;
CREATE TABLE stg.customer_payment_input (
    payment_date date null,
    payment_number varchar(50) null,
    customer varchar(1000) null,
    invoice_number varchar(50) null,
    payment_amount money null,
    deduction_amount money null,
    account_code varchar(200) null,
    memo varchar(200) null
);

do $$
declare
files text[]:=array[~<PARAMETER_2>~];
copy_command text;
x text;
begin
drop table if exists stg.tmp_customer_payment_input;
CREATE TABLE stg.tmp_customer_payment_input (
    payment_date date null,
    payment_number varchar(50) null,
    customer varchar(1000) null,
    invoice_number varchar(50) null,
    payment_amount money null,
    deduction_amount money null,
    account_code varchar(200) null,
    memo varchar(200) null
);

FOREACH x IN ARRAY files
  LOOP
    copy_command := 'copy stg.tmp_customer_payment_input from '''|| x || '''delimiter '','' csv header';
    execute copy_command;

    insert into stg.customer_payment_input
    select * from stg.tmp_customer_payment_input;

    truncate stg.tmp_customer_payment_input;
  END LOOP;
  drop table stg.tmp_customer_payment_input;
end;
$$;

drop table if exists stg.data_issue_customer_payment ;
create table stg.data_issue_customer_payment
as
SELECT *
FROM stg.customer_payment_input
WHERE UPPER(BTRIM(memo)) = UPPER('invoice apply')
AND (payment_amount is null OR invoice_number is null);

drop table if exists stg.customer_payment;
create table stg.customer_payment as
SELECT *
FROM stg.customer_payment_input
where UPPER(BTRIM(memo)) != UPPER('invoice apply')
OR (payment_amount is not null AND invoice_number is not null);

/*
======output #1====================================================
historical + current-month invoice left join current-month payment.
===================================================================
*/

--validation to be added.
--we are assuming there is no duplicate invoice_numbers for every customer.
DROP TABLE IF EXISTS stg.OUTPUT;
create table stg.output as
WITH CTE AS (
    SELECT
        a.customer,
        a.date as invoice_date,
        a.po_number,
        a.invoice_number,
        a.sku,
        a.quantity,
        a.unit_price,
        a.quantity * a.unit_price as subtotal,
        sum(a.quantity * a.unit_price)over(partition by a.invoice_number) as invoice_total,
        a.quantity * a.unit_price * COALESCE(D.discount, 0) as discount_amount,
        coalesce(b.payment_amount, a.payment_received) as payment_amount,
        coalesce(b.payment_date, a.payment_date) as payment_date,
        coalesce(b.payment_number, a.payment_number) as payment_number,
        not (a.has_been_paid_before
            and (b.payment_amount is not null
                or b.payment_date is not null
                or b.payment_number is not null)) as is_valid
    FROM (
        SELECT customer,
            date,
            po_number,
            invoice_number,
            sku,
            quantity::numeric,
            unit_price,
            null as payment_received,
            null as payment_date,
            null as payment_number,
            false as has_been_paid_before
        FROM stg.INVOICE_SALES
        UNION all
        SELECT
            customer,
            invoice_date,
            po_number,
            invoice_number,
            sku,
            quantity::numeric,
            unit_price,
            payment_received,
            payment_date,
            payment_number,
            CASE WHEN payment_received IS NOT null
                OR payment_date IS NOT null
                OR payment_number IS NOT null THEN true
                ELSE false
            END
        FROM stg.output_from_previous_cycle
    ) AS A
    LEFT JOIN stg.CUSTOMER_PAYMENT AS B
        ON upper(btrim(A.customer)) = upper(btrim(b.customer))
        and upper(btrim(a.invoice_number)) = upper(btrim(b.invoice_number))
        and upper(btrim(b.memo)) = upper('invoice apply')
    LEFT JOIN stg.discount AS D
        ON UPPER(BTRIM(A.CUSTOMER)) = UPPER(BTRIM(D.CUSTOMER))
)
SELECT
    customer,
    invoice_date,
    po_number,
    invoice_number,
    sku,
    quantity,
    unit_price,
    subtotal,
    invoice_total,
    discount_amount,
    payment_number,
    payment_date,
    CASE WHEN unit_price = cast(0 as money) THEN cast(0 as money)
        ELSE payment_amount * (subtotal * 1.0 / invoice_total)
    END as payment_received,
    CASE WHEN unit_price = cast(0 as money) THEN cast(0 as money)
        ELSE subtotal - discount_amount - payment_amount * (subtotal * 1.0 / invoice_total)
    END as ar_balance,
    is_valid
FROM CTE
order by customer asc, invoice_date asc, po_number asc;

copy (select 
    customer,
    invoice_date,
    po_number,
    invoice_number,
    sku,
    quantity,
    unit_price,
    subtotal,
    invoice_total,
    discount_amount,
    payment_number,
    payment_date,
    payment_received,
    ar_balance
    from stg.output 
    where is_valid = true)
to '~<PARAMETER_3>~\output.csv'
delimiter ','
csv header;

/*
======output #2====================================================
payment without matched invoice
===================================================================
*/

DROP TABLE IF EXISTS stg.OUTPUT_Payment_without_invoice;
create table stg.OUTPUT_Payment_without_invoice as
SELECT
    a.*
FROM stg.CUSTOMER_PAYMENT AS A
LEFT OUTER JOIN (
    SELECT customer,
        invoice_number
    FROM stg.INVOICE_SALES
    UNION all
    SELECT
        customer,
        invoice_number
    FROM stg.output_from_previous_cycle
) AS B
    ON upper(btrim(A.customer)) = upper(btrim(b.customer))
    and upper(btrim(a.invoice_number)) = upper(btrim(b.invoice_number))
where B.customer is null;

copy stg.OUTPUT_Payment_without_invoice
to '~<PARAMETER_3>~\output_payment_without_invoice.csv'
delimiter ','
csv header;

/*
======output #3====================================================
compare the current-month payment between that from current-month
customer payment and that from output with payment data equal to 
current month.
===================================================================
*/

DROP TABLE IF EXISTS stg.output_current_cycle_payment_comparison;
create table stg.output_current_cycle_payment_comparison as
with cte_customer_payment as (
    select customer,
        sum(payment_amount) as aggregated_payment_amount
    from stg.customer_payment
    group by customer
)
, cte_distinct_year_month as (
    select distinct customer, 
        extract(year from payment_date) as year, 
        extract(month from payment_date) as month
    from stg.customer_payment
)
, cte_customer_payment_without_invoice as (
    select a.customer,
        sum(a.payment_amount) as aggregated_payment_amount
    from stg.OUTPUT_Payment_without_invoice as A
    inner join cte_distinct_year_month as B
        on upper(btrim(a.customer)) = upper(btrim(b.customer))
        and extract(year from a.payment_date) = b.year
        and extract(month from a.payment_date) = b.month
    where upper(btrim(a.memo)) = upper('invoice apply')        
    group by a.customer
)
, cte_output as (
    select a.customer,
        sum(a.payment_received) as aggregated_payment_amount
    from stg.output as A
    inner join cte_distinct_year_month as B
        on a.is_valid = true
        and upper(btrim(a.customer)) = upper(btrim(b.customer))
        and extract(year from a.payment_date) = b.year
        and extract(month from a.payment_date) = b.month
    group by a.customer
)
select 
    a.customer as "Customer from Customer_Payment",
    a.aggregated_payment_amount as "Total Payment from Customer_Payment",
    b.customer as "Customer from Output",
    b.aggregated_payment_amount as "Total Payment from Output",
    c.customer as "Customer from Customer_Payment_without_Invoice",
    c.aggregated_payment_amount as "Total Payment from Customer_Payment_without_Invoice"
from cte_customer_payment as A
full outer join cte_output as B
    on upper(btrim(a.customer)) = upper(btrim(b.customer))
full outer join cte_customer_payment_without_invoice as C
    on coalesce(upper(btrim(a.customer)), upper(btrim(b.customer))) = upper(btrim(c.customer));

copy stg.output_current_cycle_payment_comparison
to '~<PARAMETER_3>~\output_current_cycle_payment_comparison.csv'
delimiter ','
csv header;

/*
======output #4, 5, 6==============================================
4. data issue invoice sales.
5. data issue customer payment.
6. data issue invoice paid more than once.
===================================================================
*/

do $$
begin
if exists (select * from stg.data_issue_invoice_sales) then
    copy stg.data_issue_invoice_sales
    to '~<PARAMETER_3>~\data_issue_invoice_sales.csv'
    delimiter ','
    csv header;
end if;
if exists (select * from stg.data_issue_customer_payment) then
    copy stg.data_issue_customer_payment
    to '~<PARAMETER_3>~\data_issue_customer_payment.csv'
    delimiter ','
    csv header;
end if;
if exists (select * from stg.output where is_valid = false) then
    copy (select * from stg.output where is_valid = false)
    to '~<PARAMETER_3>~\data_issue_invoices_paid_more_than_once.csv'
    delimiter ','
    csv header;
end if;
end
$$;
