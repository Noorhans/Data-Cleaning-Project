##DATA CLEANING PROJECT

SELECT * FROM layoffs;

## STEP 1: remove duplicate data if there is any: 
## STEP 2: standardize the Data 
## STEP 3: explore null values/ blank values 
## STEP 4: normalize the table 

CREATE TABLE layoffs_staging LIKE layoffs; ##create a copy table to avoid losing any data 

SELECT * FROM layoffs_staging; ##created columns like the one in table layoffs
INSERT layoffs_staging SELECT * FROM layoffs; ##inserts data from layoff to layoff_staging
SELECT *,
ROW_NUMBER() OVER (partition by company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging; ## distinguishes and puts a row number by the columns mentioned in the pertition. 

## CTE creation
WITH duplicate_CTE as 
(
SELECT *,
ROW_NUMBER() OVER (partition by company, industry, total_laid_off, percentage_laid_off, `date`,location, stage, country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * FROM duplicate_CTE 
WHERE row_num > 1 
;

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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (partition by company, industry, total_laid_off, percentage_laid_off, `date`,location, stage, country,funds_raised_millions) AS row_num
FROM layoffs_staging;  ##copied all data and added row num for filter
SELECT * FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0; ## disable the safe update mode only during this session 

DELETE FROM layoffs_staging2
WHERE row_num > 1; ##delete duplicate

SELECT * FROM layoffs_staging2
WHERE row_num > 1; ##check for dups

-- data standarization: 

SELECT company, (trim(company)) 
FROM layoffs_staging2; 

UPDATE layoffs_staging2
SET company = trim(company) ##trim takes off the white space in the beginnging to make all rows in one format
;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
; 

UPDATE layoffs_staging2 
SET industry='Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1; ##no issues found 

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country= 'United States' 
where country LIKE 'United States%';

SELECT  `date`, 
  STR_TO_DATE(`date`, '%m/%d/%Y') AS formatted_date 
FROM 
  layoffs_staging2;
  
UPDATE layoffs_staging2
SET `date`=  STR_TO_DATE(`date`, '%m/%d/%Y');
  
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

 SELECT * FROM layoffs_staging2 t1
 JOIN layoffs_staging2 t2
 ON t1.company=t2.company
 AND t1.location=t2.location
 WHERE (t1.industry IS NULL OR t1.industry='') AND t2.industry IS NOT NULL
 ;
 
 UPDATE layoffs_staging2 t1
 JOIN layoffs_staging2 t2
 ON t1.company=t2.company
 SET t1.industry = t2.industry 
 WHERE t1.industry IS NULL 
 AND t2.industry IS NOT NULL
 ;
 
 
 UPDATE layoffs_staging2 
 SET industry = NULL 
 WHERE industry = '';
 
 SELECT * FROM layoffs_staging2 where company= 'Airbnb';
 
 SELECT * FROM layoffs_staging2 where total_laid_off IS NULL AND percentage_laid_off IS NULL; ## since they r null and the data isnt used for the table purpose then it can be deleted
 
 DELETE FROM layoffs_staging2 where total_laid_off IS NULL AND percentage_laid_off IS NULL;
 
 ALTER TABLE layoffs_staging2 
 DROP COLUMN row_num;