package wordExtraction

import com.kunyandata.nlpsuit.util.KunyanConf
import org.apache.spark.mllib.clustering.DistributedLDAModel
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.{SparkConf, SparkContext}
import sentiment.Util.ParseJson

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangxin on 2016/6/13.
  * 基于LDA的关键词抽取主程序
  *
  * 1、大数据训练 ->LDA模型 -> 主题-词项 分布
  * 2、新文档 +LDA 模型 -》新文档-主题分布
  * 3、通过【主题-词项】 和【新文档-主题】 计算【词项-新文档】
  * 4、然后将【词项-新文档】考虑香农熵，计算关键词-文档权重，并排序初选关键词
  * 5、提取新文档的候选关键词，匹配初选关键词，得到最终关键词
  */
object Run {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordExtraction").setMaster("local")
    val sc = new SparkContext(conf)

    //加载主题模型
    val model = DistributedLDAModel.load(sc,"D:\\333_WordExtraction\\Run_hancks\\Model")

    //加载词表
    val vocab = sc.textFile("D:\\333_WordExtraction\\Run_hancks\\Dict\\vocab.txt").collect()
    val vocabMap = vocab.zipWithIndex.toMap  //词表转成map
    val numVocabMap = vocabMap.map(_.swap)

    //主题——词分布
    println("<<<<<<<<<<<<<<<主题-词分布>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    val phi = model.topicsMatrix
    for (topic <- Range(0, model.k)) {
      println("Topic " + topic + "  概率:")
      var sum = 0.0
      for (word <- Range(0, 20)) {
        println("    " +vocab(word)+" : "+ phi(word, topic))
      }
      for (word <- Range(0, vocab.size)) {
        sum = sum+phi(word, topic)
      }
      println(sum)
    }

    //主题——词权重分布
    println("<<<<<<<<<<<<<<<主题-词权重分布>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    val weight = model.describeTopics()
    for (topic <- Range(0, model.k)) {
      println("Topic " + topic + " ")
      val topicWeight=weight(topic)
      var sum=0.0
      for (word <- Range(0, 10)) {
        println("    " + numVocabMap(topicWeight._1(word)) + "  "+ topicWeight._1(word)+ " : "+ topicWeight._2(word))
      }
      for (word <- Range(0, topicWeight._2.length)) {
        sum = sum+topicWeight._2(word)
      }
      println("   权重: "+sum)
    }

    //文档-主题分布
    println("<<<<<<<<<<<<<<<文档-主题分布>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    val theta = model.topTopicsPerDocument(model.k)
    theta.collect().foreach(line => {
      println(line._1 + ":  ")
      for(n <- Range(0, model.k)){
        println("    |    " + line._2(n) + "  "+ line._3(n))
      }
    })


    //新文档预测
    println("<<<<<<<<<<<<<<<新文档预测>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    val stopWordsPath = "D:\\333_WordExtraction\\Run\\stop_words_CN"

    //坤雁分词器配置项
    val configInfo = new ParseJson("D:\\333_WordExtraction\\Run\\config.json")
    val kunyanConfig = new KunyanConf
    kunyanConfig.set(configInfo.getValue("kunyan", "host"), configInfo.getValue("kunyan", "port").toInt)

    val doc = "周四，国内豆类品种从主力合约看，豆一1509合约收于4393元/吨，较前一日结算价下跌0.05%；豆粕1509合约收于2556元/吨，较前一日结算价上涨0.79%；豆油1509合约收于5754元/吨，较前一日结算价下跌0.31%。\n　　现货方面，河北唐山地区油厂豆粕价格：43%蛋白：2620元/吨；广西防城港地区油厂豆粕价格：43%蛋白：2530元/吨；江苏盐城地区油厂豆粕价格：43%蛋白：3040元/吨。辽宁大连：工厂报价，一级豆油5970元/吨，三级豆油5900元/吨；天津地区：中纺报价，一级豆油5950元/吨；浙江宁波：工厂报价，一级豆油5950元/吨。\n　　据美国地球卫星气象(EarthSatWeather)旗下的农业气象机构MDACropCast发布的预测数据显示，2015/16年度全球大豆产量预计为3.027亿吨，和上周发布的数据持平。相比之下，美国农业部预计2015/16年度全球大豆产量为3.173亿吨，略高于2014/15年度的3.172亿吨。MDACropCast维持2015/16年度美国大豆产量预测值不变，仍为38.23亿蒲式耳，比2014/15年度创纪录的产量减少3.6%，相比之下，美国农业部的预测为38.5亿蒲式耳。MDACropCast公司本周继续预测2015/16年度巴西大豆产量为9830万吨，阿根廷大豆产量预计为5700万吨，均和早先预测值持平。\n　　操作上，豆一1509合约冲高回落，期价连续四天收阴，短期观望为宜；豆粕1509合约超跌反弹，前期空单可以分批止盈，剩余空单依托10天均线持有；豆油1509合约上试20天线未果，短期20天均线存在一定的压制作用，趋势投资者暂时观望为宜。"
    val doc2 = "临近年中，各大车商都在盘点半年业绩表现。从豪华车市场来看，增速放缓已经成为这一细分市场的新特点。来自全国乘用车市场联席会的统计显示，今年前4个月，豪华车市场平均增速13%。尽管这一增长仍然高于乘用车市场的平均增速，但与去年同期相比增长速度明显放缓。\n　　增速放缓导致豪华车市场竞争更趋激烈，但是仍然有品牌脱颖而出。来自英菲尼迪发布的官方数据显示，5月英菲尼迪在中国内地市场(不含港、澳、台)的销量达3,969辆，连续第三个月刷新月度销量纪录，同比劲增50%。这一增幅近四倍于豪华车市场的平均增幅。\n　　东风英菲尼迪总经理戴雷博士(Dr. Daniel Kirchert)表示：“英菲尼迪良好的市场表现得益于国产车型Q50L和QX50销量的强劲增长，验证了中国消费者对英菲尼迪国产车型品质的充分认可，也体现了东风英菲尼迪的综合竞争实力。”\n　　英菲尼迪持续领跑豪华车市场\n　　今年以来，豪华车市场的增长持续放缓，从前5个月的市场表现来看，豪华车第一阵营中的德系三强，除了奔驰保持较高增长外，其他两家德系豪华车商明显放缓了增长速度。\n　　相比之下，第二阵营的豪华车品牌成为拉动市场增长的动力。其中，来自英菲尼迪官方数据显示，今年1至5月，英菲尼迪在华累计销量达15,620辆，同比增长36%，保持豪华车市场领先增速。\n　　究其原因，国产车型正在成为英菲尼迪保持强劲增长的重要引擎。今年5月，英菲尼迪Q50L和QX50销量达到2,293辆，占5月总销量的58%；从前5个月来看，两款国产车型Q50L和QX50在华累计销量达到7,618辆，占英菲尼迪在华总销量的48%，对整体销量拉升作用明显，并直接带动了英菲尼迪在华市场占有率的快速提升。\n　　同时，与其他豪华车品牌相比，英菲尼迪在营销领域更接“地气”也是原因之一。诸如英菲尼迪与深圳卫视联合出品的明星全球竞技类真人秀——《极速前进》第一季获得了良好的收视率与网络口碑，节目热播同时，英菲尼迪“敢爱”品牌精神也得到深度传播，创造与消费者在情感层面的共鸣，从而极大地提升了英菲尼迪的品牌知名度和美誉度。\n　　以强势产品力应对低增长新常态\n　　过去几年，尽管中国汽车市场整体增速趋缓，但是豪华车市场依旧保持较高增速。自2015年起，乘用车市场的低增长“新常态”开始向豪华车领域蔓延。\n　　有观点认为：“可以预期，豪华车市场增长放缓成为常态后，体量较大的德系三强仍然将继续占据主流市场，而第二集团军的竞争会越来越激烈。”\n　　在豪华车市场上增长较为快速的细分市场来看，中级轿车和小排量车型仍然是年轻消费者热衷选择的车型。对于英菲尼迪而言，在这一市场的产品布局已经完成。\n　　以国产车型高科技豪华运动座驾Q50L为例，目前这一车型进驻的是市场份额最为稳定的细分市场，Q50L搭载的2.0T发动机的动力性能足以与德系竞争对手相媲美，同时Q50L还以精准驾控、跃级豪华和全球统一的高品质更胜一筹；此外，风尚智能四驱SUV QX50也正处于快速增长的SUV细分市场区间。而近期进口引入的新Q70L则开拓了豪华商务轿车细分市场，挑战这一市场上表现强势的德系品牌。\n　　业内人士认为，英菲尼迪的产品线覆盖了增长最快的所有细分市场，同时快速导入符合市场需求的新产品，无疑将迎来在更大的成功。“目前市场增速放缓是中国汽车行业共同面对的挑战。我们将坚持本土化和品牌塑造两个长期战略，有信心持续扩大市场占有率，从而实现英菲尼迪品牌在中国市场长期健康的发展。”戴雷表示。\n　　可以预期，凭借极富情感共鸣的营销方式、日渐提升的品牌口碑以及稳定可靠的产品质量，英菲尼迪正快速改写其在豪华车阵营的排位。"
    val doc3 = "北京时间 消息 全球 最大 中文 搜索 引擎 公司 百度 Nasdaq BIDU 今天 公布 季度 未经 审计 财务 报告 \n 季度 业绩 要点 \n 季度 营业 收入 一季度 增长 18.0% 去年 同期 相比 增长 196.8% 达到 人民币 \n 净利润 第四季度 增长 43.5% 季度 增长 1309.0% 达到 人民币 季度 盈利 人民币 稀释 盈利 人民币 \n 季度 股权 报酬 费用 净利润 增长 股权 报酬 费用 利润 计算 盈利 稀释 盈利 分别为 人民币 人民币 \n 季度 超过 客户 提供 服务 一季度 增长 17.5%\n 百度 董事会 主席 首席 执行官 李彦宏 表示 百度 季度 运营 再次 表现 卓越 继续 扩大 中文 搜索 市场 份额 销售 能力 不断 加强 越来越 企业 认识到 百度 竞价 排名 网络 平台 带来 益处 百度 客户 数量 持续增长 \n 百度 首席 财务官 王湛 表示 百度 季度 营业 收入 实现 强劲 增长 春节 季节性 影响 预计 搜索 业务 模式 持续 拓展 有效 改善 营业收入 强劲 增长 季度 净利润 杰出 表现 有效地 证明 百度 商业 模式 高度 扩展性 \n 季度 财务 业绩 分析 \n 季度 营业收入 人民币 一季度 增长 18.0% 同期 增长 196.8% 面临 春节 长假 季节性 因素 百度 业绩 依然 超过 先前 乐观 估计 \n 网络 营销 收入 人民币 一季度 增长 18.2% 同比 增长 207.0% 增长 不断 增加 客户 数量 季度 百度 超过 客户 提供 服务 一季度 增长 17.5% 增长 反映 百度 直销 分销 效能 大幅 提高 \n 联盟 成本 季度 总收入 9.1% 一季度 比例 联盟 成本 收入 比重 增加 百度 联盟 成员 数量 主题 推广 业务 增长 \n 销售 管理 费用 季度 一季度 增长 2.0% 季度 增长 145.2% 研发 费用 季度 增长 8.7% 季度 增长 136.7% 研发 团队 人数 增加 股权 报酬 费用 增加 一季度 数字 数字 增长 原因 于从 执行 美国 新的 会计 准则 -SFAS \n 净利润 一季度 增长 43.5% 超过 季度 净利润 季度 盈利 人民币 稀释 盈利 人民币 \n 公司 现金 现金 等价物 人民币 季度 营业 活动 净现金 资本 性支出 分别为 人民币 \n 季度 展望 百度 预计 季度 收入 人民币 人民币 之间 季度 增长 167% 177% 预测 管理层 现有 信息 初步 估计 实际 业绩 可能 估计 有所不同 "
    val doc1 = "青少年 科学家 角逐 巨奖 \n 印第安纳波利斯 全球 国家 地区 青年 科学家 发明家 英特尔 国际 科学 工程 大奖赛 英特尔 ISEF 共同 角逐 总价值 奖学金 奖项 大赛 安捷伦 科技 公司 大力 协助 \n 参赛 选手 带来 项目 有望 彻底 解决 科学 难题 包括 代用 能源 开发 项目 孤独症 学习 障碍 治疗 项目 降低 水源 污染 项目 减少 电子 废弃物 项目 共有 项目 大奖赛 亮相 参赛 学生 有幸 科学家 会面 交流 想法 展示 发现 \n 今年 英特尔 公司 成为 ISEF 冠名 赞助商 英特尔 公司 致力 提高 全球 教育 水平 投入 资金 重要 支出 \n 英特尔 公司 教育 总监 Brenda Musilli 表示 英特尔 ISEF 不仅仅 一项 科学 竞赛 下一代 科学家 投资 过去 年中 赛事 进行 投资 参赛 人数 增加 36% 参赛 国家 数目 参赛 学生 从事 科学 项目 日趋 精深 \n 今年 决赛 选手 年龄 之间 决赛 选手 16% 以前 曾经 参加 英特尔 ISEF 15% 拥有 已经 申请 一项 美国 专利 47% 选手 女性 类别 比赛 工程 比赛 参赛 项目 最多 环境 科学 医药 健康 三大 参赛 项目 数量 大赛 项目 总数 40% \n 英特尔 ISEF 奖项 \n 大赛 决赛 选手 设置 众多 奖项 包括 \n 英特尔 基金会 青年 科学家 决赛 三名 获得 大学 奖学金 \n Seaborg Stockholm 国际 青年 科学 研讨会 SIYSS 评选 类别 冠军 获奖者 参加 举办 SIYSS 活动 诺贝尔 颁奖 典礼 \n 类别 比赛 类别 设有 大奖 设置 团队 大奖 大奖 包括 三和 比赛 得分 最高 学生 获得 类别 冠军 一台 英特尔 迅驰 移动 计算 技术 笔记本 电脑 \n 英特尔 ISEF 背景 信息 \n 国际 科学 工程 大奖赛 ISEF 起由 英特尔 主办 全世界 最大 中学生 科学 大赛 汇聚 全世界 前途 年级 青少年 科学家 发明家 学生 印第安纳波利斯 进行 最后 角逐 全球 学年 参加 科学 大奖赛 选手 胜者 学生 一起 参加 超过 全球 地区性 英特尔 ISEF 联席 科学 大赛 赢得 英特尔 ISEF 参赛 \n 专家 志愿者 学生 作品 进行 评判 英特尔 ISEF 评委 必须 拥有 学科 博士 学位 学历 领域 拥有 至少 相关 专业 经验 \n 科学 服务社 一直 管理 英特尔 ISEF 科学 服务 一家 全球 享有盛誉 组织 致力于 出版物 教育 计划 促进 全世界 了解 科学 重视 科学 了解 科学 服务社 英特尔 ISEF 更多 信息 访问 www sciserv org \n 实施 英特尔 ISEF 教育 项目 英特尔 致力 激发 培养 全球 社区 儿童 科学 数学 工程 领域 兴趣 了解 更多 信息 访问 www intel com/education \n 英特尔 公司 芯片 创新 开发 技术 产品 计划 全球 领先 厂商 致力于 不断 改进 工作 生活 方式 了解 英特尔 更多 信息 访问 www intel com/pressroom "
    val doc4 = "网上 店主 易趣 网上 公开 拍卖 盗版 四库 全书 电子版 连累 易趣网 官司 四库 全书 电子版 版权 香港 文化 出版 有限公司 认为 易趣网 允许 人士 网上 拍卖 盗版 四库 全书 行为 实际上 帮助 盗版 行为 实施 侵权 行为 易趣网 所属 公司 法庭 要求 立即 停止 侵权 行为 提出 经济 赔偿 市二中 开庭 审理 \n 文渊阁 四库 全书 电子版 香港 文化 出版 有限公司 自主 研发 电子 产品 获得 版权 登记 确认 其著 公司 公司 发现 商家 易趣 网上 公开 拍卖 四库 全书 电子版 拍卖 方式 一口价 人民币 正版 光盘 售价 经查 商家 销售 四库 全书 电子版 公司 授权 正常 销售 渠道 盗版 公司 易趣 公司 法庭 \n 　迪 公司 认为 提供 交易 平台 易趣网 网上 销售 盗版 产品 未尽 合理 审查 义务 行为 下载 盗版 复制 四库 全书 电子版 提供 渠道 便利 促使 盗版 复制 拍卖 得以 实施 实际上 参与 帮助 盗版 行为 实施 侵权 行为 要求 易趣 公司 立即 停止 侵权 行为 原告 登报 道歉 赔偿 经济 损失 \n 庭审 法庭 提出 四库 全书 电子版 著作权 原告 上海人民出版社 共同 原告 表示 追加 上海人民出版社 共同 原告 法院 开庭 再审 \n 网络 平台 责任 争议 \n 网上 销售 侵权 产品 提供 销售 平台 网站 究竟 没有 责任 法庭上 双方 围绕 争议 焦点 展开 辩论 \n 易趣网 并不 存在 侵权 行为 公司 易趣 公司 请求 法院 驳回 易趣 公司 代理人 辩称 易趣网 信息 传播 平台 并不 介入 买卖 双方 交易 之中 易趣网 并不 存在 侵权 行为 事发 易趣网 已经 删除 销售 盗版 四库 全书 网页 已经 合理 审查 义务 应该 承担 责任 \n 　迪 公司 帮助 实施 侵权 行为 侵权 易趣网 辩解 公司 不予 认可 代理人 列举 易趣网 侵权 事实 互联网 未经 原告 许可 拍卖 版权 作品 互联网 发布 虚假 广告 网页 抄袭 原告 产品 宣传 图片 资料 行为 侵犯 原告 享有 计算机 软件 著作权 复制 发行权 信息 网络 传播 李晓明 "
    val doc5 = "山海关 古城 开发 古建筑 修缮 施工 注意 情况 \n 　在 修缮 过程 重点 修缮 局部 复原 古建筑 强调 沿用 我国 建筑 技术 传统 技艺 科学 方法 经验 老工人 相结合 发挥 操作 技术 方面 地方 手法 介绍 修缮 古建筑 文物 价值 结构 特点 修缮 要求 进行 技术 交流 方法 交流 弥补 时代 特征 文物 知识 方面 不足 强调 严谨 工作 方法 凡需 拆卸 构件 都应 登记 编号 做出 标志 构件 应分项 建立 检修 登记册 名称 编号 规格 木质 检修 情况 加固 方法 应该 登记 体现 以备 修补 加固 工程 竣工 装入 档案 存查 残缺 构件 事先 进行 登记 备料 拆卸 需要 检查 加固 复制 补齐 草图 登记 备查 更替 下来 有价值 构件 应由 山海关 长城 博物馆 收藏 研究 展示 使用 达到 尽量 使用 原有 构件 做到 原件 原状 原构 原位 目标 加固 残损 构件 残坏 部分 工作 重点 引进 实验 可靠 技术 还原 残损 部件 原有 形制 规格 强化 结构 功能 加固 残损 构件 残坏 部分 技术 难题 需要 设法 攻克 修缮 工作 已经 实验 施工 构件 加固 造价 新更 构件 造价 原有 构件 历史 遗物 古建筑 文物 原状 价值 组成部分 加固 构件 保存 文物 原状 价值 对此 不能 单纯 经济 造价 计算 选自 山海关 古城 保护 开发 新闻 发布会 著名 长城 专家 中国长城学会 秘书长 董耀会 山海关 古城 保护 开发 问题 回答 记者 提问 \n "
    val doc6 = "《爱情公寓》系列曾是“假期金牌节目”！没看到《爱情公寓》就如同没有放假一样，已经大红大紫的四季让人也无限期待第五部的开拍。可是天意弄人，就在眼看《爱5》要来之际，王牌曾小贤的扮演者陈赫却发生了离婚，出轨等负面新闻，所以《爱5》的拍摄以及人员一直是人们的关注点。"

    //新文档预处理
    val segDoc = Array{doc}
    val (cvModel,countVectors)= Pretreat.doc2VectorsPredict(sc, segDoc, stopWordsPath, kunyanConfig, vocab)

    //预测得到[新文档-主题]分布
    val result = model.toLocal.topicDistributions(countVectors)
    result.collect().foreach(line => println(line._1 + ":  " + line._2))

    //获取新文档词表
    val newVocab = cvModel.vocabulary

    //获取 [新文档-主题] 矩阵
    val topicVectorArr = result.collect().map(_._2)
    val topicPerDocVector = topicVectorArr(0).toArray

    //[新文档-主题]矩阵 输出
    var count = 0
    topicPerDocVector.foreach(line => {
      println("  |  "+count+"  : "+line)
      count = count+1
    })

    //获取[新文档关键词-词频] 矩阵
    val wordFreArr = countVectors.collect().map(_._2)
    val wordFre = wordFreArr(0).toArray

    //关键词抽取并输出
    wordExtractionByWeight(doc, model, vocab, newVocab, vocabMap, weight, topicPerDocVector, wordFre)
  }

  /**
    * 关键词抽取并输出
    * @param doc 新文档
    * @param model LDA模型
    * @param vocab LDA模型的词表
    * @param newVocab 新文档词表
    * @param vocabMap LDA模型的词表[关键词-编号]键值对
    * @param weight 主题-词项 权重矩阵
    * @param topicPerDocVector 新文档-主题分布
    * @param wordFre 新文档词频 矩阵
    */
  def wordExtractionByWeight(doc:String, model: DistributedLDAModel, vocab: Array[String], newVocab: Array[String], vocabMap:Map[String,Int],
                             weight: Array[(Array[Int], Array[Double])], topicPerDocVector: Array[Double], wordFre: Array[Double]): Unit ={

    val wordPerDocResult = newVocab.map(word=>{

      //1、计算权值
      var prob_w_d = 0.0
      //如果这个单词存在于词表
      if(vocab.contains(word)){
        for(topic <-Range(0,model.k)){

          val topicWeight = weight(topic)
          val wordId = topicWeight._1.indexOf(vocabMap(word))

          val prob_w_z = topicWeight._2(wordId)   //关键词与主题的权重
          val prob_z_d = topicPerDocVector(topic)
          prob_w_d = prob_w_d + prob_w_z * prob_z_d
        }
      }else{
        //如果不存在于词表，则为0
        prob_w_d = prob_w_d + 0
      }

      //2、计算香农值
      var wordNum = 0.0
      if(vocab.contains(word)){
        val wordId = vocabMap(word)
        wordNum = wordFre(wordId)
      }

      val result = if(prob_w_d !=0.0) -wordNum*math.log(prob_w_d) else 0.0

      println(word + "  : "+ wordNum + "  " + prob_w_d)

      word -> result
    })

    //排序输出n个关键词
    val n= 10
    val words = new ArrayBuffer[String]()
    val sortTemp = wordPerDocResult.sortBy(_._2).toBuffer
    sortTemp.trimStart(sortTemp.size-n)
    for(n <- Range(0,sortTemp.size)) {
      println("....||=" + sortTemp(sortTemp.size - n - 1)._1 + "  :  " + sortTemp(sortTemp.size - n - 1)._2)
      words += sortTemp(sortTemp.size - n - 1)._1
    }

    //结合候选关键词中提取最终关键词
    println("[[结合候选关键词中提取最终关键词(未排序)]]>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    val candiWord = CandidateWord.candiWord(doc)
    candiWord.foreach(line => {
      if(words.contains(line._1) || words.contains(line._2)) println("....|| "+line._1+line._2)
    })

    //按照words排序进行输出
    println("[[按照words排序进行输出]]>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    words.foreach(word => {
      candiWord.foreach(line => {
        if(word.equals(line._1) || word.equals(line._2)) println("....|| "+line._1+line._2)
      })
    })

    //按照words排序进行输出（剔除一些重复项）
    println("[[按照words排序进行输出(并剔除一些重复项)]]>>>>>>>>>>>>>>>>>>>>>>>>")
    words.foreach(word =>{
      var countFlag=0
      candiWord.foreach(line =>{
        if(countFlag == 0  && (word.equals(line._1) || word.equals(line._2))) {
          println("....|| " + line._1 + line._2)
          countFlag = 1
        }
      })
      countFlag = 0
    })

  }

  /**
    * 通过 [主题-词项] 概率值
    * @param model
    * @param vocab
    * @param newVocab
    * @param vocabMap
    * @param phi 主题-词项 概率分布
    * @param topicPerDocVector
    * @param wordFre
    */
  def wordExtraction(model: DistributedLDAModel, vocab: Array[String], newVocab: Array[String], vocabMap: Map[String, Int],
                     phi: Matrix, topicPerDocVector: Array[Double], wordFre:Array[Double]): Unit = {

    val wordPerDocResult = newVocab.map(word=>{

      //1、计算权值
      var prob_w_d = 0.0

      //如果这个单词存在于词表
      if(vocab.contains(word)){
        for(topic <-Range(0,model.k)){
          val prob_w_z = phi(vocabMap(word),topic)   //关键词与主题的概率值
          val prob_z_d = topicPerDocVector(topic)
          prob_w_d = prob_w_d + prob_w_z * prob_z_d
        }
      }else{
        //如果不存在于词表，则为0
        prob_w_d = prob_w_d + 0
      }

      //2、计算香农值
      var wordNum = 0.0
      if(vocab.contains(word)){
        val wordId = vocabMap(word)
        wordNum = wordFre(wordId)
      }

      val result = if(prob_w_d !=0.0) -wordNum*math.log(prob_w_d) else 0.0
      println(word +"  : "+ wordNum+"  "+ prob_w_d)

      word -> result
    })

    //排序输出n个关键词
    val n = 10
    val sortTemp = wordPerDocResult.sortBy(_._2).toBuffer
    sortTemp.trimStart(sortTemp.size-n)              //截掉前面不需要的部分
    for(n <- Range(0,sortTemp.size)) {
      println("....||="+sortTemp(sortTemp.size-n-1)._1+"  :  "+sortTemp(sortTemp.size-n-1)._2)
    }

    //------------------------【不考虑香农熵】------------------------------
    println("[[不考虑香农熵]]>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    val wordPerDocResult2 = newVocab.map(word=>{

      //1、计算权值
      var prob_w_d = 0.0
      //如果这个单词存在于词表
      if(vocab.contains(word)){
        for(topic <-Range(0,model.k)){
          val prob_w_z = phi(vocabMap(word),topic)
          val prob_z_d = topicPerDocVector(topic)
          prob_w_d = prob_w_d + prob_w_z * prob_z_d
        }
      }else{
        //如果不存在于词表，则为0
        prob_w_d = prob_w_d + 0
      }
      (word -> prob_w_d)
    })

    //排序输出n个关键词
    val sortTemp2 = wordPerDocResult2.sortBy(_._2).toBuffer
    sortTemp2.trimStart(sortTemp2.size-n)
    for(n <- Range(0,sortTemp2.size)) {
      println("....||="+sortTemp2(sortTemp2.size-n-1)._1+"  :  "+sortTemp2(sortTemp2.size-n-1)._2)
    }
  }

  def wordExtraction(vocab:Array[String],newVocab:Array[String],vocabMap:Map[String,Int], wordFre:Array[Double]): Unit ={
    println("[[直接通过词频提取]]>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    val wordFreMap = newVocab.map(word=>{
      var wordNum = 0.0
      if(vocab.contains(word)){
        val wordId = vocabMap(word)
        wordNum = wordFre(wordId)
      }

      (word -> wordNum)
    })

    val n = 10
    val sortTemp2 = wordFreMap.sortBy(_._2).toBuffer
    sortTemp2.trimStart(sortTemp2.size-n)
    for(n <- Range(0,sortTemp2.size)) {
      println("....||=" + sortTemp2(sortTemp2.size-n-1)._1+"  :  " + sortTemp2(sortTemp2.size-n-1)._2)
    }

  }
}
