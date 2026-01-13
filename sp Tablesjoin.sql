use Internship_Projects
go

drop procedure if exists Tablesjoin;
go 

create procedure Tablesjoin

@Id nvarchar(55)
as
begin
set nocount on;
declare  @perfID nvarchar(55)
set  @perfID = @Id;

drop table if exists Student_Performance               ----created two identical tables excluding where clausor 
drop table if exists Student_Performance_object

select p.Id, p.Foreign_Id, p.Students, p.Math, p.Physics, p.Language, p.Academic_level,
d.Year, d.Semester, d.Month, p.Gender, a.City, a.Address,
CASE WHEN p.Math >= 50 THEN 'Pass' ELSE 'Fail' END AS Math_Status,
CASE WHEN p.Physics >= 50 THEN 'Pass' ELSE 'Fail' END AS Physics_Status,
CASE WHEN p.Language >= 50 THEN 'Pass' ELSE 'Fail' END AS Language_Status,
((p.Math + p.Physics + p.Language)/3) AS Overal_Average, 
AVG(p.Math) over() AS MathsAverage, AVG(p.Physics) over() AS PhysicsAverage, Avg(p.Language) over() as LanguageAverage,
CASE 
    WHEN ((p.Math + p.Physics + p.Language) / 3) >= 50 THEN 'Pass'
    ELSE 'Fail' 
END AS Result_Status,
    CASE 
        WHEN gender = 'Male' THEN 'Male' 
        ELSE NULL 
    END AS Male_C,
    CASE 
        WHEN gender = 'Female' THEN 'Female' 
        ELSE NULL 
    END AS Female_C,

CASE 
    WHEN ((p.Math + p.Physics + p.Language) / 3) >= 90 THEN 'Excellent'
    WHEN ((p.Math + p.Physics + p.Language) / 3) >= 75 THEN 'Good'
    WHEN ((p.Math + p.Physics + p.Language) / 3) >= 50 THEN 'Average'
    WHEN ((p.Math + p.Physics + p.Language) / 3) >= 30 THEN 'Need Support'
    ELSE 'No Effort'
END AS Grading,

CASE 
    WHEN (CASE WHEN p.Math < 30 THEN 1 ELSE 0 END +                 -------boolean counting for true or false
          CASE WHEN p.Physics < 30 THEN 1 ELSE 0 END + 
          CASE WHEN p.Language < 50 THEN 1 ELSE 0 END) >= 2 
    THEN 'Failing'
    WHEN (CASE WHEN p.Math <= 50THEN 1 ELSE 0 END +                 -------boolean counting for true or false
        CASE WHEN p.Physics <= 50 THEN 1 ELSE 0 END + 
        CASE WHEN p.Language <= 60 THEN 1 ELSE 0 END) >= 2 
    then 'At Risk'
    ELSE 'On Track'
END AS Progress,


COUNT(*) OVER() AS Total_Students
into Student_Performance
from performance p
inner join Dates d on p.Foreign_Id = d.Id
inner join Area a on p.Foreign_Id = a.Id


select p.Id, p.Foreign_Id, p.Students, p.Math, p.Physics, p.Language, p.Academic_level,
d.Year, d.Semester, d.Month, p.Gender, a.City, a.Address,
CASE WHEN p.Math >= 50 THEN 'Pass' ELSE 'Fail' END AS Math_Status,
CASE WHEN p.Physics >= 50 THEN 'Pass' ELSE 'Fail' END AS Physics_Status,
CASE WHEN p.Language >= 50 THEN 'Pass' ELSE 'Fail' END AS Language_Status,
((p.Math + p.Physics + p.Language)/3) AS Overal_Average, 
AVG(p.Math) over() AS MathsAverage, AVG(p.Physics) over() AS PhysicsAverage, Avg(p.Language) over() as LanguageAverage,
CASE 
    WHEN ((p.Math + p.Physics + p.Language) / 3) >= 50 THEN 'Pass'
    ELSE 'Fail' 
END AS Result_Status,
CASE 
    WHEN ((p.Math + p.Physics + p.Language) / 3) >= 90 THEN 'Excellent'
    WHEN ((p.Math + p.Physics + p.Language) / 3) >= 75 THEN 'Good'
    WHEN ((p.Math + p.Physics + p.Language) / 3) >= 50 THEN 'Average'
    ELSE 'Below Average'
END AS Grading,

CASE 
    WHEN (CASE WHEN p.Math < 50 THEN 1 ELSE 0 END +                 -------boolean counting for true or false
          CASE WHEN p.Physics < 50 THEN 1 ELSE 0 END + 
          CASE WHEN p.Language < 50 THEN 1 ELSE 0 END) >= 2 
    THEN 'Needs Support'
    ELSE 'On Track'
END AS Intervantion
into Student_Performance_object
from performance p
inner join Dates d on p.Foreign_Id = d.Id
inner join Area a on p.Foreign_Id = a.Id
where p.Id = @perfID;   -----exlcluded clauser at the first table

--select * from Student_Performance
select * from Student_Performance_object ---- need to do select statement so that you can see results everytime you run

END
go 

execute Tablesjoin '5f43f996-12e8-4b18-9b21-398950325523';


