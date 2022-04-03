# 第四&五周HIVE作业
## 题目一（简单）
### 展示电影 ID 为 2116 这部电影各年龄段的平均影评分。
```sql
select age, avg(rate) avgrate 
from t_user tu 
join t_rating tr on tu.userid = tr.userid 
where movieid='2116'
group by age 
order by age;
```
### 输出结果
![image](https://user-images.githubusercontent.com/16860476/161422331-a0304edc-1046-4759-8f09-f0d53a41b92d.png)

## 题目二（中等）
### 找出男性评分最高且评分次数超过 50 次的 10 部电影，展示电影名，平均影评分和评分次数。
```sql
select sex, moviename name, avg(rate) avgrate, count(1) total 
from t_user tu 
join t_rating tr on tu.userid = tr.userid 
join t_movie tm on tr.movieid = tm.movieid
where sex='M'
group by sex, moviename
having total>50 
order by avgrate desc 
limit 10;
```
### 输出结果
![image](https://user-images.githubusercontent.com/16860476/161422370-f0023e5b-fa11-459f-a4f0-cb0911b3ab18.png)

## 题目三（选做）
### 找出影评次数最多的女士所给出最高分的 10 部电影的平均影评分，展示电影名和平均影评分（可使用多行 SQL）。
```sql
select moviename, avg(rate) avgrate from (
select movieid from (
select tu.userid, count(1) total
from t_user tu 
join t_rating tr on tu.userid = tr.userid 
where sex='F'
group by tu.userid
order by total desc 
limit 1
) a
join t_rating tr on a.userid = tr.userid
order by rate desc, movieid
limit 10
) b
join t_rating tr on b.movieid = tr.movieid
join t_movie tm on tr.movieid = tm.movieid
group by moviename;
```
### 输出结果
由于有超过10个电影评分5分的，所以根据join和排序顺序，结果不唯一
![image](https://user-images.githubusercontent.com/16860476/161422437-6cc40c1a-d009-4cb8-89f9-a4af8952f45f.png)
