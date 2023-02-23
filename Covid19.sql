/*
Covid 19- Data Exploration 
Skills used: Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types
*/

--extracting data's of both covid deaths and vaccinated 

SELECT * 
FROM Covid19..Covid_deaths$
WHERE continent is not null
ORDER BY 1,2


SELECT * 
FROM Covid19..Covid_vaccination$
ORDER BY 1,2

-- Total Cases vs Total Deaths
-- Shows the likelihood of dying if you contact covid in your country

select location, date, total_cases, new_cases, total_deaths, population
from Covid19..Covid_deaths$
order by 1,2


-- percentage of population infected with Covid

select location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as Percentpopulationinfected
from Covid19..Covid_deaths$
where location like '%United States%'
order by 1,2

--Country with highest infection rates based on populaton

select location, population, MAX(total_cases) as Highestinfectcount, MAX((total_cases/population))*100 as Percentpopulationinfected
from Covid19..Covid_deaths$
group by location, population 
order by Percentpopulationinfected desc

-- countries with highest death count

Select location, MAX(cast(total_deaths as int)) as Totaldeathcount
From Covid19..Covid_deaths$
Group by location
order by Totaldeathcount desc

-- Grouping by Continents
--Continents with highest death count per population

Select continent, MAX(cast(total_deaths as int)) as Totaldeathcount
From Covid19..Covid_deaths$
Where continent is not null
Group by continent
order by Totaldeathcount desc

-- GLOBAL NUMBERS

select date, SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(new_cases)*100 as Deathpercentage
from covid19..Covid_deaths$
where continent is not null
order by 1,2 

--total cases, calculating deathpercentage

select SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(new_cases)*100 as Deathpercentage
from covid19..Covid_deaths$
where continent is not null
order by 1,2

--Comparing the total population vs vaccination

Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.location Order by dea.location, dea.Date) as Rollingpeoplevaccinated
-- (Rollingpeoplevaccinated/population)*100
from covid19..Covid_deaths$ dea
Join covid19..Covid_vaccination$ vac
   on dea.location = vac.location
   and dea.date =  vac.date
where dea.continent is not null
order by 2,3


-- CTE

with PopvsVac (continent, location, date, population, new_vaccinations, rollingpeoplevaccinated)
as
(
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.location Order by dea.location, dea.date) as rollingpeoplevaccinated
-- (Rollingpeoplevaccinated/population)*100
from covid19..Covid_deaths$ dea
Join covid19..Covid_vaccination$ vac
   on dea.location = vac.location
   and dea.date =  vac.date
where dea.continent is not null
--order by 2,3
)
select *, (rollingpeoplevaccinated/population)/100
from PopvsVac

--CREATING TABLE

DROP Table if exists #percentpopulationvaccinated
Create Table #percentpopulationvaccinated
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
new_vaccinations numeric,
rollingpeoplevaccinated numeric
)

Insert into #percentpopulationvaccinated  
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (Partition by dea.location Order by dea.location, dea.date) as rollingpeoplevaccinated
-- (Rollingpeoplevaccinated/population)*100
from covid19..Covid_deaths$ dea
Join covid19..Covid_vaccination$ vac
    on dea.location = vac.location
    and dea.date = vac.date
where dea.continent is not null
--order by 2,3

Select *, (rollingpeoplevaccinated/population)*100 as percent_vaccinated
From #percentpopulationvaccinated



-- Creating View to view data for later 

Create View percentpopulationvaccinated as
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.location Order by dea.location, dea.date) as rollingpeoplevaccinated
--, (RollingPeopleVaccinated/population)*100
From covid19..Covid_deaths$ dea
Join covid19..Covid_vaccination$ vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null

Select *
From #percentpopulationvaccinated
