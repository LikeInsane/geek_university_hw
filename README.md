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
SELECT s.*
FROM student s
WHERE s.s_id NOT IN
    (SELECT st.s_id
    FROM student st
    LEFT JOIN score sc
        ON sc.s_id=st.s_id
    LEFT JOIN course c
        ON c.c_id=sc.c_id
    LEFT JOIN teacher t
        ON t.t_id=c.t_id
    WHERE t.t_name="张三" ) ;
```
![image](https://user-images.githubusercontent.com/16860476/213989355-74d1fd76-594b-4f50-bbf3-ecbdb31b22b7.png)

## 九. 查询学过编号为"01"并且也学过编号为"02"的课程的同学的信息
```sql
SELECT s.*
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
                ON a.s_id = b.s_id) c
        JOIN student s
        ON c.s_id = s.s_id;
```
![image](https://user-images.githubusercontent.com/16860476/213991056-417e057f-c4ed-498b-a277-bf5dac79b017.png)

## 十. 查询学过编号为"01"但是没有学过编号为"02"的课程的同学的信息
```sql
SELECT c.* from
    (SELECT *
    FROM 
        (SELECT s_id
        FROM score
        WHERE c_id = '01') a
        WHERE s_id NOT IN 
            (SELECT s_id
            FROM score
            WHERE c_id = '02') ) b
        JOIN student c
        ON b.s_id = c.s_id;
```
![image](https://user-images.githubusercontent.com/16860476/213993549-03445d1c-0e03-4d2f-b71a-40157207500a.png)

## 十一. 查询没有学全所有课程的同学的信息
```sql
SELECT student.*
FROM student, 
    (SELECT st.s_id
    FROM student st
    LEFT JOIN score AS sc
        ON st.s_id=sc.s_id
    GROUP BY  st.s_id
    HAVING count(distinct sc.c_id)<
        (SELECT count(c_id)from course))as s
        WHERE student.s_id=s.s_id;
```
![image](https://user-images.githubusercontent.com/16860476/213996712-04f0c876-471a-46f1-9a44-abffc7a29ef0.png)

## 十二. 查询至少有一门课与学号为"01"的同学所学相同的同学的信息
```sql
SELECT DISTINCT b.*
FROM 
    (SELECT s_id
    FROM score
    WHERE c_id IN 
        (SELECT c_id
        FROM score
        WHERE s_id = '01')) a
    JOIN student b
    ON a.s_id = b.s_id
        AND a.s_id!='01';
```
![image](https://user-images.githubusercontent.com/16860476/213998196-614de41f-2f63-4c82-91df-806c22cfe4b4.png)

## 十三. 查询和"01"号的同学学习的课程完全相同的其他同学的信息
```sql
SELECT a.*
FROM student a
WHERE a.s_id IN 
    (SELECT s_id
    FROM score
    WHERE s_id != '01'
            AND c_id IN 
        (SELECT c_id
        FROM score
        WHERE s_id = '01')
        GROUP BY  s_id
        HAVING count(c_id)= 
            (SELECT count(c_id)
            FROM score
            WHERE s_id = '01'));
```

## 十四. 查询没学过"张三"老师讲授的任一门课程的学生姓名
```sql
SELECT st.s_name
FROM student st
WHERE st.s_id NOT IN 
    (SELECT sc.s_id
    FROM score sc
    JOIN course c
        ON c.c_id=sc.c_id
    JOIN teacher t
        ON t.t_id=c.t_id
            AND t.t_name="张三" );
```
![image](https://user-images.githubusercontent.com/16860476/214008354-13445eb1-999a-486f-9beb-ebc812a6c794.png)

## 十五. 查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩
```sql
SELECT a.s_id,
         a.s_name,
         b.avg_score
FROM student a
JOIN 
    (SELECT s_id,
         AVG(s_score) avg_score
    FROM score
    WHERE s_score<60
    GROUP BY  s_id
    HAVING count(c_id)>=2 ) b
    ON a.s_id=b.s_id;
```
![image](https://user-images.githubusercontent.com/16860476/214011213-400b29e9-8628-48ad-89ab-f80e55e7ab80.png)
