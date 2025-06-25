# DATA CLEANING ----


show databases;
use layoffs_info;
select * from layoffs;
select * from layoffs_staging;
insert layoffs_staging select * from layoffs;
select * from layoffs_staging;



select *,
row_number () over (
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date',stage,country, funds_raised_millions) as row_num from layoffs_staging;

with duplicate_cte as
(
select *,
row_number () over (
partition by company,location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num from layoffs_staging
)
 select * from duplicate_cte where row_num > 1;



#with duplicate_cte as
#(
#select *,
#row_number () over (
#partition by company,location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num from layoffs_staging
#)
 #delete from duplicate_cte where row_num > 1;
 #tidak berjalan pakai delete itu maka pakai yang bawah ini

#INI MEMAKAI  --> table layoffs_staging klik kanan -->copy to clipboard --> create the statement --> klik paste
 CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
select * from layoffs_staging2;
insert into layoffs_staging2
select *,
row_number () over (
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date',stage,country, funds_raised_millions) as row_num from layoffs_staging;

select * from layoffs_staging2;
set sql_safe_updates=0;
delete from layoffs_staging2 where row_num>1;
set sql_safe_updates=1;
select * from layoffs_staging2;
select * from layoffs_staging2 where row_num= 1;
select * from layoffs_staging2;


#--STANDARDIZING DATA

select distinct(company) from layoffs_staging2;

select company, trim(company) from layoffs_staging2;

set sql_safe_updates=0;
update layoffs_staging2 set company = trim(company);
set sql_safe_updates=1;
select company, trim(company) from layoffs_staging2;
select * from layoffs_staging2;

select distinct(industry) from layoffs_staging2 order by 1;
select * from layoffs_staging2;
select * from layoffs_staging where industry like 'Crypto%';
set sql_Safe_updates=0;
update layoffs_staging2 set industry = 'Crypto' where industry like 'Crypto%';
set sql_safe_updates=1;
select * from layoffs_staging2;
select * from layoffs_staging2 where industry like 'Crypto%';

show databases;
use layoffs_info;
show tables;
select * from layoffs_staging2;
select distinct(industry) from layoffs_staging2;
select * from layoffs_staging2;
select distinct(location) from layoffs_staging2;
select * from layoffs_staging2;
select distinct(country) from layoffs_staging2;
select * from layoffs_staging2;
select * from layoffs_staging2 where country like 'United States%';
select distinct(country) from layoffs_staging2 where country like 'United States%';
select * from layoffs_staging2 where country like 'United States%';

select distinct country, trim(trailing '.' from country) from layoffs_staging2 order by 1;

set sql_safe_updates =0;
update layoffs_staging2 set country = trim(trailing '.' from country);
set sql_safe_updates=1;
select * from layoffs_staging2;
select distinct(country) from layoffs_staging2;
select * from layoffs_staging2;

#-- Now we want to change the column date  which is a text type into the real 'date' using this one
 select `date`, str_to_date(`date`,'%m/%d/%Y') from layoffs_staging2;
 select * from layoffs_staging2;
 set sql_safe_updates=0;
 update layoffs_staging2 set `date` = str_to_date(`date`,'%m/%d/%Y');
 set sql_safe_updates=1;
 select * from layoffs_staging2;
 
 -- mengubah tipe dari date (text) ke date (date)
 alter table layoffs_staging2 modify column `date` date;
 
 -------- # Mengolah data NULL or ' ' (blank)
 select * from layoffs_staging2;
 select * from layoffs_staging2 where industry is null or industry ='';
 select * from layoffs_staging2;
 select * from layoffs_staging2 where company= 'Airbnb';

show databases;
use layoffs_info;
show tables;
select * from layoffs_staging2;

#----COLUMN YANG NULL AKAN DIISI SESUAI DENGAN DATA LAIN YG SAMA

#-----Menampilkan 2 tabel yaitu t1 data dari kolom industry yg null dan t2 itu tabel yg kolom industrynya
#ada yg terisi dengan company yg sama
select *
from layoffs_staging2 t1 
join layoffs_staging2 t2  
    on t1.company=t2.company
where (t1.industry is null or t1.industry='')
and t2.industry is not null;

#-----MENGUBAH DATA ''(BLANK) KE NULL KARENA KALAU TIDAK BEGINI DATA YG AKAN DIISI TIDAK DPT DIUPDATE
update layoffs_staging2
set industry = null 
where industry ='';


#---MENAMPILKAN INDUSTRY DARI T1 DAN T2 SESUAI DENGAN KONDISI INI
select t1. industry, t2.industry
from layoffs_staging2 t1 
join layoffs_staging2 t2  
    on t1.company=t2.company
where (t1.industry is null or t1.industry='')
and t2.industry is not null;

#----MENGISI(UPDATE) KOLOM INDUSTRY YANG NULL (T1) DENGAN KOLUM YG ADA DATANYA(T2)
set sql_safe_updates=0;
update layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company=t2.company
set t1.industry= t2.industry
where t1.industry is null
and t2.industry is not null;

#--Mengecek apakah sudah terisi
select * from layoffs_staging2;
select * from layoffs_staging2 where company = 'Airbnb';

#---Mengecek apakah masih ada industry yg null
select * from layoffs_staging2;
select * from layoffs_staging2 where industry is null;
#Tidak diproses karena tidak ada company(data) yg samayg menjadi patokan look at this below
select * from layoffs_staging2;
select * from layoffs_staging2 where company like 'Bally%';

#--Mengecek apakah ada data total dan percentage yg null krn kalau null tdk bisa diolah dong gaada informasinya
select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

#--Menghapus data total dna percentage yg null krn gabisa diolah datanya
delete from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;
#cek lagi
select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;
select * from layoffs_Staging2;

#MENGHAPUS KOLUM row_num krn ya sudah tdk perlu lagi
alter table layoffs_staging2 drop column row_num;
select * from layoffs_staging2;
select count(*) from layoffs_staging2;


#------EXPLORATORY DATA BEGIN!!
select max(total_laid_off) from layoffs_staging2;
select * from layoffs_staging2 where total_laid_off =12000;
select max(total_laid_off), max(percentage_laid_off) from layoffs_Staging2;
select * from layoffs_staging2 where percentage_laid_off=1;
select * from layoffs_staging2 where percentage_laid_off=1 order by total_laid_off desc;

#Show total laid of order by company
select company, sum(total_laid_off) from layoffs_staging2 group by company order by 2 desc;
#order by 2 ---> order berdasarkan kolom kedua

#show date paling awal dan akhir
select * from layoffs_staging2;
select min(`date`), max(`date`) from layoffs_staging2;

select * from layoffs_staging2;
select industry, sum(total_laid_off) from layoffs_staging2 group by industry order by 2 desc;

#show data laid off based on country
select * from layoffs_staging2;
select country, sum(total_laid_off) from layoffs_staging2 group by country order by 2 desc; 

#number of laid off per year
select * from layoffs_staging2; 
select year(`date`), sum(total_laid_off) from layoffs_staging2 group by year(`date`) order by 1 desc;

#group y stage
select * from layoffs_staging2;
select stage, sum(total_laid_off) from layoffs_staging2 group by stage order by 2 desc;

#---Mencari SUM LAID OFF BERDASARKAN MONTH
select * from layoffs_staging2;
select substring(`date`,6,2), sum(total_laid_off) from layoffs_staging2 group by substring(`date`,6,2) order by 1 asc;
#PENJELASAN CODE--> substring(`date`,6,2)-->substring(`2023-02-30`,6,2)->artiny karakter ke 6 dari awal yg dimulai dari 1 dan diambil 2 angka
#contohnya gini 2023-02-30-->(2,0,2,3,-,0,2,-,3,0) karakter ke-6 nya 0 dan diambil 2 angka jd 02 bukan karakter ke-5 soalnya karakter ke-5 isinya (-) which is bukan month

#BISA JUGA GINI
select * from layoffs_staging2;
select substring(`date`,6,2) as month, sum(total_laid_off) from layoffs_staging2 group by month order by 1 asc;

#BISA JUGA KALO MAU MONTH PER YEAR
select * from layoffs_staging2;
select substring(`date`,1,7) as month, sum(total_laid_off) as 'num of laid off' from layoffs_staging2 group by month order by 1 asc;
select substring(`date`,1,7) as 'month', sum(total_laid_off) as 'num of laid off' from layoffs_staging2 where 'month' is not null group by month order by 1 asc;

show databases;
use layoffs_info;
show tables;
select * from layoffs_staging2; 
select * from layoffs_staging2;
with Rolling_total as
(
select substring(`date`,1,7) as MONTH, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by substring(`date`,1,7)
order by MONTH asc
)
select MONTH, total_off, sum(total_off) over (order by MONTH) as rolling_total from Rolling_total; 


select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
order by 3 desc;

with company_year(company, years, total_laid_off) as
( 
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
) ,
company_years_rank as
(select *, dense_rank() over (partition by years order by total_laid_off desc) as rangking 
from company_year 
where years is not null order by rangking 
)
select * from company_years_rank;

with company_year(company, years, total_laid_off) as
( 
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
) ,
company_years_rank as
(select *, dense_rank() over (partition by years order by total_laid_off desc) as rangking 
from company_year 
where years is not null order by rangking 
)
select * from company_years_rank where rangking <= 5;

show databases;
use layoffs_info;
show tables;
select * from layoffs_staging2;

create table layoffs_staging3 like layoffs_staging2;
insert into layoffs_staging3 select * from layoffs_staging2;
select * from layoffs_staging3;


set sql_safe_updates=0;
update layoffs_staging3 set percentage_laid_off ='' where percentage_laid_off is null;

select percentage_laid_off, trim(percentage_laid_off) from layoffs_staging3;
update layoffs_staging3 set percentage_laid_off=trim(percentage_laid_off);

