<h2>1、爬虫主程序入口com.gengyun.entry.OnSparkKicker</h2><br/>
<h2>2、RDDURLQueue并未添加过滤功能</h2><br/>
<h2>3、添加深度控制功能</h2><br/>
<h2>4、添加协议控制</h2><br/>
<h2>5、添加后缀控制</h2><br/>
<h2>6、以tachyon作为已爬取数据存储</h2><br/>
<h2>7、链接去重</h2>

集群模式构建
mvn package -P clusterdep -Dmaven.test.skip=true
