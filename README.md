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



