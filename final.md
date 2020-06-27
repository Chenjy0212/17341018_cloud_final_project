**项目名称**：课程设计作业_PageRank的MapReduce实现

**项目编号**：FINAL	proj

**学生姓名**：陈景宇

**学生学号**：17341018

**院系专业**：数据科学与计算机超算

**上交日期**：2020/06/27





[TOC]

# 1	选题及配置环境

​	**基于Hadoop平台设计编写一个MapReduce程序，实现PageRank算法(1.4)**

​	在 Windows 10 系统下利用 VMware Workstation 15 Pro 虚拟机管理平台，搭建ubuntu_20.04的linux系统、Hadoop 分布式基础架构等⼯具，利⽤ MapReduce 的分布式设计思想，实现PageRank 算法。



# 2	github仓库地址：





# 3	代码汇总

由两部分组成，分别是**NodeCoverter**和**PageRank**



## **3.1	NodeCoverter文件夹**

具体内容参看具体文件，把数据集节点指向关系的格式转换

- NodeCoverter.java

  这部分代码主要是进行数据处理，进行格式上的转化，以方便后续的迭代工作。

  数据格式被转化成：**“起始节点 ID + PageRank值 + SPACE + ⽬标节点1 ID + ⽬标节点2 ID + ⽬标节点3 ID……”**。

  

- NodeCoverterMapper.java

  找到split 之后起始节点的目标节点们。

  

- NodeCoverterReduce.java

  把 shuffle 之后的结果聚合到⼀起



NodeConverter 设置有关参数——输⼊⽂件夹、输出⽂件夹、节点 IP、设置 Map Task 的 Mapper  类、设置 Reduce Task 的 Reducer 类、输出 key 类、输出 value 类。



## **3.2	PageRank文件夹**

具体内容参看具体文件，对网络节点的 PageRank 值进行了排序

- PageRank.java

  起始节点的 pr 值对⽬标节点 pr 值进行贡献，并不停进⾏迭代。PageRank值通常需要迭代30-40次才能达到收敛。数据格式为

  ：**“起始节点 ID + PageRank值 + SPACE + ⽬标节点1 ID + 目标节点2 ID + 目标节点3 ID……”**。

  

- PageRankMapper

   对数据进行了平均加权

  

- PageRankReduce.java

   对起始节点对⽬标节点的贡献进行了计算。还原格式**“起始节点 ID + PageRank值 + SPACE + ⽬标节点1 ID + ⽬标节点2 ID + ⽬标节点3 ID……”**。新的数据将会作为下⼀次迭代的输⼊数据。



PageRank 设置有关参数——输⼊⽂件夹、输出⽂件夹、节点 IP、设置 Map Task 的 Mapper 类、设置 Reduce Task 的 Reducer 类、输出 key 类、输出 value 类。每⼀次 PageRank 的输出数据作为输入数据在下⼀次 MapReduce 中进⾏迭代，每⼀次迭代的结果被写⼊不同的带有序号的目标⽂件夹，便于区分不同次迭代的结果。



## **3.3	数据集**

soc-Epinions1.txt:	https://snap.stanford.edu/data/soc-Epinions1.html 

如果不能链接到数据集，先进入https://snap.stanford.edu 再查找。

存放位置为NodeConverter文件夹下

![image-20200627172055614](C:\Users\10262\AppData\Roaming\Typora\typora-user-images\image-20200627172055614.png)



# 4	实验操作过程

​	具体参看**17341018\_陈景宇\_选题1.4.pdf**



# 5	参考资料

- PageRank从原理到实现https://www.cnblogs.com/rubinorth/p/5799848.html

- 拉里佩奇（Larry Page）和谢尔盖布林（Sergey Brin）PageRank诞生论文http://117.128.6.27/cache/ilpubs.stanford.edu:8090/422/1/1999-66.pdf?ich_args2=526-27165814027831_36f78e68b7488dab6de4a2d694e14743_10001002_9c896d2fd5c7f9d29733518939a83798_dcb1517d6473dbb979abe1b28d47e474
- 浅谈PageRankhttps://blog.csdn.net/guoziqing506/article/details/70702449
- PageRank算法的研究和改进https://www.baidu.com/link?url=9Hs9Lpk7PxdYeaKGjQ9oVIFTC0-aSh2-f4Q93K9Wn35c6Tst1tKgeJNjmEV3nkCcQBnVhGCWyGLs6skBLLWZ2KFFUa_p7DH4ru-x3IdPpSWmPHsK-2uEGSsliSbDBAqF&wd=&eqid=f0cf34c90002aec4000000065ef70af3

