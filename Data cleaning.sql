#Data cleaning, total_lay_off, hacer un staging que es como una copia trabajable la tabla original, vigilar estar trabajando en el staging
# evita errores en el doc original en caso de que se necesite de nuevo. 


# 1. remover duplicados
# 2. estandarizar los datos, ortogafria y demas
# 3.  mirar los null o blank 
# 4. remover cosas o columnas innesesarias


#CREACIÓN DE UNA TABLA SECUNDARIA PARA FACILITAR EL TRABAJO

SELECT *
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;


#remover los duplicados, revisar con cuidado que si sean duplicados en todas las categorias, de aqui para abajo las formulas 
SELECT *
FROM layoffs_staging;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = "Casper";


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;

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

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

#ESTANDARIZACIÓN 

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);


SELECT DISTINCT industry
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";

SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING "." FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE  layoffs_staging2
SET country = TRIM(TRAILING "." FROM country)
WHERE country LIKE 'United States%';

SELECT `date`,
STR_TO_DATE(`date`, "%m/%d/%Y")
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, "%m/%d/%Y");

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

# NULL VALUES 

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

#de aqui para abajo el codigo para llenar información a partir de una tbala que ya la tiene

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = "";

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = "";

SELECT *
FROM layoffs_staging2
WHERE company = "Airbnb";

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = "")
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;


#OTRO TEMA, REMOVER NULL AND BLANKS

SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

#REMOVER COLUMNAS 
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

#analisis exploratorio de los datos, hacer nuevas comparaciones, buscar por fechas, laidoffs, prcentajes, paises, 
#buscar datos que pueden tener elguna influencia entre ellos.

SELECT *
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT *
FROM layoffs_staging2
WHERE country = "Colombia";

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2;

#esta formula sirve para la sumatoria y cualquier categoria, este fecha filtrada x año, 
#pero puede ser usada para el stage, country e industry

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR (`date`)
ORDER BY 1 DESC;

#este sirve para comparar dos cosas, usando ell distinct ayuda a que solo haya un row/categoria
SELECT industry, 
COUNT(DISTINCT company) AS comp_count
FROM layoffs_staging2
GROUP BY industry
ORDER BY comp_count DESC, industry ASC;


SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY `MONTH`ASC;


# da una tabla con la fecha, los despidos mensuales y la sumatoria de los despidos, tener cuidado con el substring 
WITH rolling_total AS
(SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY `MONTH`ASC
)
SELECT `MONTH`, total_off,
SUM(total_off) OVER(ORDER BY `MONTH`) AS r_t
FROM rolling_total;


# La suma de todos los laid off por compañia 


SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company 
ORDER BY 2 DESC;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

#organiza los datos donde compara compañia, año, total lay_off, los presenta de forma tal que 
#rankea por año, pone el puesto que ocupo esa compañia en cuanto a la cantidad 
#de despidos que hizo, por ejemplo en el 2020 uber fue el primero, 2021 fue bytedance
#en el 2023 fue google, este lo organiza como 2020, 2021, 2022, 2023, 2020 y asi sucesivamente 
#hasta que se completen todos los rankings


WITH com_ye (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
)
SELECT *, 
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS R_T
FROM com_ye
WHERE years IS NOT NULL
ORDER BY R_T ASC;

#este hace lo mismo que el otro, pero presenta el año como grupos, primero 2020, luego 2021,
#el dense rank funciona por año, el menor que 5 hace que se resuma en las 5 compañias que mas despidos tuvieron en x año

WITH com_ye (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), company_year_rack AS
(SELECT *, 
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS R_T
FROM com_ye
WHERE years IS NOT NULL
)
SELECT*
FROM company_year_rack
WHERE R_T<= 5;





