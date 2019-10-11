//第一步：从本地ubuntu系统或hdfs读取数据
//注意：这里我的数据是放在自己的本地电脑上的，所以textFile的读入路径是本地路径，
// 但是如果数据是放在HDFS上的话，读入路径要变成相应的hdfs路径

//==>下面的pre_data实质上是一个rdd
val pre_data = sc.textFile("/home/huang/ComputerApplying/SparkWorkSpace/f20674.dat")


//第二步：对读取的数据进行清洗，经过观察数据发现，这些数据字段之间的空白有占一个字符和两个
// 字符的，可以使用replace进行替换。另外，由于有些数据缺失或者异常，要使用rdd的filter操作进行过滤
val hsy_fields = pre_data.map( line=>line.trim().replace("  "," ").replace("  "," ").split(" ") ).filter(_.length==15).filter(_(13)!="9").filter(_(11)!="999.9")                                        


//第三步：对清洗的数据获取年份、降水量这两个字段后进行map操作，再对该map按照年份进行聚合求平均值

val hsy_pre_ann = hsy_fields.map( hsy_fields => (hsy_fields(1), hsy_fields(11).toDouble) ).groupByKey().map{ x=>(x._1, x._2.reduce(_+_) ) }

//第四步：对取得的平均值进行排序，注意，这里使用了一个小技巧：先把map的键值位置对换，对换时需要对值
// 乘以天数365，然后使用，然后使用sortByKey进行排序，排序后再次把map的位置对换，就得到
// 按照每年降水量降序排序的降水量数据

val hsy_pre_sort = hsy_pre_ann.map(x=> (x._2*365, x._1) ).sortByKey(false).map(x=>(x._2, x._1) )

// 第五步：将排序的结果输出到指定目录中（一个全新的目录才行，不能是已经存在的）
hsy_pre_sort.saveAsTextFile("/home/huang/ComputerApplying/SparkWorkSpace/TuJieSparkCoreTech")









































