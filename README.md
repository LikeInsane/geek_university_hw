# 第一次作业
## 一. 查询"01"课程比"02"课程成绩高的学生的信息及课程分数
```sql
SELECT s.*,
         s_score
FROM 
    (SELECT a.s_id,
         a.s_score
    FROM 
        (SELECT *
        FROM score sc
        WHERE sc.c_id = '01') a
        JOIN 
            (SELECT *
            FROM score sc
            WHERE sc.c_id = '02') b
                ON a.s_id = b.s_id
            WHERE a.s_score > b.s_score) c
        JOIN student s
        ON c.s_id = s.s_id;
```
![image](https://user-images.githubusercontent.com/16860476/212036498-498ae9ee-e703-4702-ae98-78fc54d6d234.png)

## 二. 查询"01"课程比"02"课程成绩低的学生的信息及课程分数
```sql
SELECT s.*,
         s_score
FROM 
    (SELECT a.s_id,
         a.s_score
    FROM 
        (SELECT *
        FROM score sc
        WHERE sc.c_id = '01') a
        JOIN 
            (SELECT *
            FROM score sc
            WHERE sc.c_id = '02') b
                ON a.s_id = b.s_id
            WHERE a.s_score < b.s_score) c
        JOIN student s
        ON c.s_id = s.s_id;
```
![image](https://user-images.githubusercontent.com/16860476/212461103-588953f5-4782-4131-8014-9d6eedab1d2e.png)

## 三. 查询平均成绩大于等于 60 分的同学的学生编号和学生姓名和平均成绩
```sql
SELECT a.s_id,
         s_name,
         avg_score
FROM 
    (SELECT s_id,
         sum(s_score)/4 avg_score
    FROM score
    GROUP BY  s_id) a
JOIN student b
    ON a.s_id = b.s_id
        AND avg_score >= 60;
```
![image](https://user-images.githubusercontent.com/16860476/212462454-437e30d1-46a5-4e5d-9f87-a0a941a1792a.png)

## 四. 查询平均成绩小于 60 分的同学的学生编号和学生姓名和平均成绩
```sql
SELECT a.s_id,
         s_name,
         avg_score
FROM 
    (SELECT s_id,
         sum(s_score)/4 avg_score
    FROM score
    GROUP BY  s_id) a
JOIN student b
    ON a.s_id = b.s_id
        AND avg_score < 60;
```
![image](https://user-images.githubusercontent.com/16860476/212462567-ab0984f0-c161-41ec-85e3-6f71c133fc82.png)

## 五. 查询所有同学的学生编号、学生姓名、选课总数、所有课程的总成绩
```sql
SELECT a.s_id,
         s_name,
         course_nums,
         score_all
FROM 
    (SELECT s_id,
         count(c_id) course_nums,
         sum(s_score) score_all
    FROM score
    GROUP BY  s_id) a
JOIN student b
    ON a.s_id = b.s_id;
```
![image](https://user-images.githubusercontent.com/16860476/212462212-8628363a-3eee-474e-bfde-dd845ee0c2f7.png)

## 六. 查询"李"姓老师的数量
```sql
SELECT count(t_id) t_nums
FROM teacher
WHERE t_name LIKE '李%';
```
![image](https://user-images.githubusercontent.com/16860476/212462702-02310f90-1fc8-4990-aeb8-ac8d5f978563.png)

## 七. 查询学过"张三"老师授课的同学的信息
```sql
SELECT d.*
FROM (select * from teacher where t_name = '张三') a
JOIN course b
    ON a.t_id = b.t_id
JOIN score c
    ON b.c_id = c.c_id
JOIN student d
    ON c.s_id = d.s_id; 
```
![image](https://user-images.githubusercontent.com/16860476/212463304-0195ac4a-5f09-4c5c-ba71-8090b6053615.png)

## 八. 查询没学过"张三"老师授课的同学的信息
```sql
SELECT d.*
FROM (select * from teacher where t_name != '张三') a
JOIN course b
    ON a.t_id = b.t_id
JOIN score c
    ON b.c_id = c.c_id
JOIN student d
    ON c.s_id = d.s_id; 
```
![image](https://user-images.githubusercontent.com/16860476/212464322-0277c59b-889f-4c66-8a99-343835ea35db.png)

