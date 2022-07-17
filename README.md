# 第十五周Flink作业
## 題目
report(transactions).executeInsert(“spend_report”);
将 transactions 表经过 report 函数处理后写入到 spend_report 表。

每分钟（或小时）计算在五分钟（或小时）内每个账号的平均交易金额（滑动窗口）？使用分钟还是小时作为单位均可。

##### 代碼:
![image](https://user-images.githubusercontent.com/16860476/179397395-8e73a022-9745-40b5-8884-44fd46d2b4b5.png)


### 输出结果
![image](https://user-images.githubusercontent.com/16860476/179397632-c66f9737-6687-4b2d-8976-09edb3b351ed.png)

![image](https://user-images.githubusercontent.com/16860476/179397424-004c2fc0-1e70-4b1c-9a71-786d3540ca38.png)

