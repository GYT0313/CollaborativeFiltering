package com.gyt.collaborativeFiltering;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @FileName: CollaborativeFilteringByUser.java
 * @Package: com.gyt.collaborativeFiltering
 * @Author: Gu Yongtao
 * @Description: 使用协同过滤推荐算法，基于用户，实现电影推荐
 *
 ********************************************************************************************************************
 * 代码分析：
 * main函数：
 * 	共执行三个函数：
 * 	1. generateSourceData(locaFilePath) 
 * 		根据已有的用户名、电影名生成用户对电影的随机评分，按照 用户	电影1$评分;电影2$评分;....格式写入本地磁盘
 * 	
 * 	2. putFile2HDFS(locaFilePath)
 * 		将generateSourceData函数写入本地的磁盘文件上传到 HDFS 
 * 
 * 	3. startMapReduce(inputOutputPath)
 * 		启动MapReduce，为用户推荐电影
 * 		MapReduce程序共分为：ColFilterMapper、ColFilterCombiner、ColFilterReducer
 * 		1) ColFilterMapper
 * 			执行次数：行数
 * 			输入：读取HDFS文件，每次按行读入
 * 			输出：key=电影名，value=用户1$评分;用户2$评分.....
 * 			内部功能：
 * 				1. 按照读取的顺序，将用户名赋值给users一维数组，其下标作为用户ID
 * 				2. 将用户的对所有电影的评分写入usersInfo变量（key=用户名，value=map类型，map类型中：key=电影名，value=评分）
 * 
 * 
 * 		2) ColFilterCombiner
 * 			执行次数：Mapper输出的不同key值数量（相同key将合并）
 * 			输入：Mapper的输出
 * 			输出：key=ID&用户 value=电影$评分;电影$评分;...（value值这里并没有实际意义，因为在Reducer并没有使用到)
 * 			内部功能：
 * 				1. 将输入数据的key值即电影名，赋值到一维数组movies
 * 				2. 将各用户对同一部电影的评分做差值，并将 5-该差值 作为用户与用户之间的相似度并写入二维数组（维度：用户数×用户数），
 * 					并且二维数组的下标与用户ID一一对应。该值越大，用户越相似。按照key=电影名，value=各用户之间的
 * 					相似度二维数组的格式put到 userSimilarityMap变量。
 * 				3. 按照用户数量输出（因为如果输出有重复，Reducer最后写入文件会出现重复）
 * 
 *
 * 		3) ColFilterReducer
 * 			输入：Combier的输出：key=ID&用户 value=电影$评分;电影$评分;...
 * 			输出：key=用户名	value=推荐电影列表
 * 			内部功能：
 * 				1. userSimilarityMap变量记录了不同电影下用户与用户之间的相似度值，将其求和，即将userSimilarityMap变量
 * 					的所有value值（二维数组）对应位置求和。得到总的用户相似度二维数组 userSimilarityAll（维度：用户数×用户数）
 * 				2. userSimilarityAll二维数组中记录了用户与用户之间的总相似度，其值越高越相似。根据Combiner传过来的key=ID&用户，
 * 					按照key值中的ID确定userSimilarityAll数组中的行，然后遍历该行，找出相似度值最大的3个用户（修改变量值即可确定
 * 					寻找前几个最相似的用户），符合条件的用户所在位置的列下标值即为用户的ID。根据此ID可通过users一维数组确定用户名。
 * 					根据用户名可通过usersInfo集合确定用户对所有电影的评分。获取到最相似的3各用户的所有评分后，
 * 						按照：电影推荐度 = 用户1对该电影评分×用户与目标用户的总相似度值 
 * 						+ 用户2对该电影评分×用户与目标用户的总相似度值
 * 						+ 用户3对该电影评分×用户与目标用户的总相似度值。
 * 					最后按照以下条件，确定某电影是否推荐给目标用户：
 * 						1) 目标用户对该电影评分为0.0，即为看过该电影
 * 						2) 该电影的推荐值大于所有电影对该用户的推荐度的平均值
 * 
 ***********************************************************************************************************************
 * @Date: 2018年12月18日 下午5:11:33
 */

public class CollaborativeFilteringByUser {
	// 属性
	// 用户下标变量 mapper中使用
	public static int userIndex = 0;
	// 用户下标变量 combiner中使用
	public static int userIndex2 = 0;
	// 电影下标变量 combiner中使用
	public static int movieIndex = 0;
	// 用户总数
	public static int userCounts = 0;
	// 电影总数
	public static int movieCounts = 0;

	// 用户名
	public static String[] users = null;

	// 电影名
	public static String[] movies = null;

	// 用户相似度集合, key=电影名, value=各用户相似度
	public static Map<String, Double[][]> userSimilarityMap = new HashMap<>();;

	// 用户信息，外层map：key=用户名 values = 电影-评分 , key = 电影 value = 评分
	public static Map<String, Map<String, String>> usersInfo = new HashMap<>();

	// 第一次执行reducer
	public static int firstRun = 0;
	// 用户之间的总相似度
	public static Double[][] userSimilarityAll = null;


	/**
	 * 
	 * @Title：main
	 * @Description: 主函数
	 * @Param: @param args
	 * @Return: void
	 * @Date: 2018年12月19日
	 */
	public static void main(String[] args) {
		System.out.println("generate data");
		// 先随机生成用户电影评分(没有实际意义，生成的数据将写与本地文件，执行MapReduce时将读取本地文件)
		String locaFilePath = "/home/hadoop/file/collaborativeFiltering/userMovieScore.txt";
		generateSourceData(locaFilePath);

		System.out.println("put to hdfs");
		// 上传源数据到HDFS
		putFile2HDFS(locaFilePath);

		System.out.println("start mapreduce");
		// 配置并启动Hadoop
		// 输入输出路径
		String[] inputOutputPath = { "hdfs://master:9000/data/colFilter", "hdfs://master:9000/output/colFileter" };
		startMapReduce(inputOutputPath);
	}

	/**
	 * 
	 * @throws @Title：generateSourceData
	 * @Description: 生成实验数据
	 * @Param:
	 * @Return: void
	 * @Date: 2018年12月18日
	 */
	private static void generateSourceData(String outputFilePath) {
		// 用户列表
		ArrayList<String> usersname = new ArrayList<>();
		// 本地磁盘用户文件地址
		String usersPath = "/home/hadoop/file/collaborativeFiltering/Users.txt";
		// 读取用户文件内容到usersname列表
		getInfoFromLocalFile(usersname, usersPath);

		// 电影列表
		ArrayList<String> moviesname = new ArrayList<>();
		// 本地磁盘电影文件地址
		String moviesPath = "/home/hadoop/file/collaborativeFiltering/Movies.txt";
		// 读取电影文件内容到moviesname列表
		getInfoFromLocalFile(moviesname, moviesPath);

		// 获取用户数，电影数
		userCounts = usersname.size();
		movieCounts = moviesname.size();

		// 初始化
		users = new String[userCounts];
		// 初始化
		movies = new String[movieCounts];

		// 生成电影评分
		// 行-用户; 列-每部电影评分
		Double[][] score2Movie = new Double[userCounts][movieCounts];
		// 初始化
		for (int i = 0; i < userCounts; i++) {
			for (int j = 0; j < movieCounts; j++) {
				score2Movie[i][j] = 0.0;
			}
		}
		// 生成各用户对各电影的评分
		generateScore(score2Movie, userCounts, movieCounts);
		// 格式化写入本地文件
		formatWrite2LocalFile(usersname, moviesname, score2Movie, outputFilePath);

	}
	
	/**
	 * 
	 * @Title：formatWrite2LocalFile
	 * @Description: 格式化写入本地文件
	 * @Param: @param usersname
	 * @Param: @param moviesname
	 * @Param: @param score2Movie
	 * @Param: @param outputFilePath
	 * @Return: void
	 * @Date: 2018年12月20日
	 */
	private static void formatWrite2LocalFile(ArrayList<String> usersname, ArrayList<String> moviesname,
			Double[][] score2Movie, String outputFilePath) {
		// 按照 用户(Tab)电影1$评分;电影2$评分;电影3$评分;...... 格式写入本地磁盘
		try {
			File file = new File(outputFilePath);
			if (file.exists()) { // 存在则先删除
				file.delete();
			}
			file.createNewFile();

			// 字节流写入磁盘
			@SuppressWarnings("resource")
			FileOutputStream oStream = new FileOutputStream(file);

			for (int i = 0; i < userCounts; i++) {
				String content = usersname.get(i) + "\t";
				for (int j = 0; j < movieCounts; j++) {
					// 拼接为：用户(Tab)电影1$评分;电影2$评分;电影3$评分;......
					content += moviesname.get(j) + "$" + score2Movie[i][j] + ";";
				}
				content += "\r\n";
				byte[] buffer = content.getBytes();
				// 写入磁盘
				oStream.write(buffer, 0, buffer.length);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @Title：getInfoFromLocalFile
	 * @Description: 读取本地文件内容到列表
	 * @Param: @param usersname
	 * @Return: void
	 * @Date: 2018年12月20日
	 */
	private static void getInfoFromLocalFile(ArrayList<String> list, String filePath) {
		try {
			// 向列表加入值，共8
			File usersFile = new File(filePath);
			if (!usersFile.exists()) {
				System.out.println("--- " + filePath + " doesn't exist! ---");
				return ;
			}
			FileReader fileReader = new FileReader(filePath);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String strLine = null;
			// IO流按行读取文件
			while ((strLine = bufferedReader.readLine()) != null) {
				strLine = strLine.trim();	// 去掉字符串头尾空格
				list.add(strLine);
			}
			
			bufferedReader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @Title：generateScore
	 * @Description: 随机生成电影评分
	 * @Param: @param score2Movie
	 * @Return: void
	 * @Date: 2018年12月18日
	 */
	private static void generateScore(Double[][] score2Movie, int userCounts, int moviesCounts) {
		// 评分为 1-5, 0代表未看过该电影--即为可能推荐的电影之一
		int alreadyWatchCounts = (moviesCounts*4)/5; // 假设每位用户看过的电影数为电影总数的4/5
		int indexOfAlreadyWatch = 0;

		// 实例化随机类
		Random random = new Random();

		for (int i = 0; i < userCounts; i++) { // i 用户
			for (int j = 0; j < alreadyWatchCounts; j++) { // j 假设一看电影数
				// 随机看过的电影下标
				indexOfAlreadyWatch = random.nextInt(moviesCounts);
				// 已评分则跳过
				if (score2Movie[i][indexOfAlreadyWatch] != 0.0) {
					j--;
					continue;
				}

				// 随机浮点型评分, 取一位小数
				DecimalFormat df = new DecimalFormat("#.0");
				Double score = Double.parseDouble(df.format(random.nextDouble() * 4 + 1));

				// 为假设已看电影写入评分
				score2Movie[i][indexOfAlreadyWatch] = score;
			}
		}

	}

	/**
	 * 
	 * @Title：putFile2HDFS
	 * @Description: 上传源文件到HDFS
	 * @Param: @param configuration
	 * @Return: void
	 * @Date: 2018年12月19日
	 */
	public static void putFile2HDFS(String sourceFilePath) {
		//
		Configuration configuration = new Configuration();
		URI uri;
		try {
			uri = new URI("hdfs://master:9000");
			FileSystem fs = FileSystem.get(uri, configuration);
			// 源文件地址
			Path src = new Path(sourceFilePath);
			// HDFS存放位置
			Path dst = new Path("/data/colFilter");
			// 判断是否存在数据目录
			if (fs.exists(dst)) {
				fs.copyFromLocalFile(src, dst);
			} else {
				fs.mkdirs(dst);
				fs.copyFromLocalFile(src, dst);
			}
		} catch (IOException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @Title：startMapReduce
	 * @Description: 启动MapReduce
	 * @Param:
	 * @Return: void
	 * @Date: 2018年12月19日
	 */
	private static void startMapReduce(String[] inputOutputPath) {
		// Hadoop配置
		Configuration configuration = new Configuration();

		Job job;
		try {
			// 如果存在输出目录，则删除
			FileSystem fileSystem = new Path(inputOutputPath[1]).getFileSystem(configuration);
			if (fileSystem.exists(new Path(inputOutputPath[1]))) {
				fileSystem.delete(new Path(inputOutputPath[1]), true);
			}

			job = Job.getInstance(configuration);
			// 设置类
			job.setJarByClass(CollaborativeFilteringByUser.class);
			job.setMapperClass(ColFilterMapper.class);
			job.setCombinerClass(ColFilterCombiner.class); // Combiner
			job.setReducerClass(ColFilterReducer.class);

			// maper输出格式
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			// reducer输出格式
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// 输入输出路径
			FileInputFormat.addInputPath(job, new Path(inputOutputPath[0]));
			FileOutputFormat.setOutputPath(job, new Path(inputOutputPath[1]));

			// 判断
			if (job.waitForCompletion(true)) {
				System.out.println("Job success!");
			} else {
				System.out.println("Job failed!");
			}
		} catch (IOException | ClassNotFoundException | InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	/**
	 * 
	 * <p>
	 * Title: ColFilterMapper
	 * </p>
	 * <p>
	 * Description: Mapper类
	 * </p>
	 * 
	 * @author Gu Yongtao
	 * @date 2018年12月18日
	 */
	public static class ColFilterMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("Start Mapper:");
			/*
			 * context的输出格式为 key=电影 value=用户$评分;用户$评分;...... (Mapper自带将相同key聚合的功能)
			 */
			// 分割数据
			StringTokenizer sTokenizer = new StringTokenizer(value.toString());
			// 用户
			String username = sTokenizer.nextToken();
			users[userIndex++] = username; // 将用户名赋值给用户数组

			// 电影$评分;电影$评分;
			String movieAndScores = sTokenizer.nextToken();

			// 分割电影$评分
			sTokenizer = new StringTokenizer(movieAndScores, ";");

			// 记录用户信息
			Map<String, String> movieScore = new HashMap<>();
			while (sTokenizer.hasMoreTokens()) {
				// 获得电影名和评分
				String string = sTokenizer.nextToken();
				// $的下标
				int index = string.indexOf("$");
				String movie = string.substring(0, index);
				String score = string.substring(index + 1);

				// 按 key=电影名， value=评分写入集合
				movieScore.put(movie, score);
				
				// key = 电影 value=用户$评分用户$评分....
				context.write(new Text(movie), new Text(username + "$" + score));
			}
			// 按照 key=用户名 value=map，map中 key=电影名， value=评分写入集合
			usersInfo.put(username, movieScore);
		}

	}

	/**
	 * 
	 * <p>
	 * Title: ColFilterCombiner
	 * </p>
	 * <p>
	 * Description: Combiner类
	 * </p>
	 * 
	 * @author Gu Yongtao
	 * @date 2018年12月18日
	 */
	public static class ColFilterCombiner extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("Start Combiner:");
			/**
			 * context的输出格式为 users的下标+用户 电影$评分;电影$评分;.... 
			 */
			// 计算用户相似度
			// 在同一部电影中计算用户与用户之间的评分差值，作为标准
			// 记录电影名
			movies[movieIndex++] = key.toString();

			// 将该电影用户评分写出到数组
			String[] usersScoreData = new String[userCounts];
			int num = 0;
			for (Text userScore : values) {
				usersScoreData[num++] = userScore.toString();
			}
			// 计算用户与用户之间的评分差值 组成usrCounts * userCounts的数组
			Double[][] userSimilarity = new Double[userCounts][userCounts];
			// context的输出
			String username = "";	// 直接赋值null会报错
			String userScore = "";
			for (int i = 0; i < userCounts; i++) {
				// 分割，i为行
				String userScore1 = usersScoreData[i];
				int index1 = userScore1.indexOf("$");
				String user1 = userScore1.substring(0, index1);
				//System.out.println(user1);	// 如果输出的顺序与 users的顺序相同则正确
				String score1 = userScore1.substring(index1 + 1);
				// 将遍历的用户名赋值给username
				if (i == userIndex2) {
					username += user1;
					userScore += score1;
				}
				for (int j = 0; j < userCounts; j++) {
					// 分割，j为列
					String userScore2 = usersScoreData[j];

					/**
					 *  同一用户相似度赋值为5-电影评分差值
					 *  因为用户评分差值越小，则二则越相似
					 *  用5来减，是因为 两个用户评分的差值最大不超过5
					 */
					if (userScore2.equals(userScore1)) {
						userSimilarity[i][j] = 0.0;
						continue;
					}
					// 分割
					int index2 = userScore2.indexOf("$");
//					String user2 = userScore2.substring(0, index2);
					String score2 = userScore2.substring(index2 + 1);

					// 5 - 两个不同用户评分之差的绝对值 = 该电影下用户与用户之间的相似度
					userSimilarity[i][j] = 5 - Math.abs(Double.parseDouble(score1) - Double.parseDouble(score2));
				}
			}
			
			// 写入map集合 key=电影  value=各用户在该电影中的评分相似度
			userSimilarityMap.put(key.toString(), userSimilarity);
			
			int userID = 0;
			// 获取用户对应在users数组中的下标
			for (int k = 0; k < userCounts; k++) {
				if (username.equals(users[k])) {
					userID = k;
					break;
				}
			}
			/*
			 * 使用判断是为了避免context重复写入，因为combiner会执行movieCounts次，
			 * reducer会执行userCounts次，若不加判断最后会重复输出
			 */
			if (userIndex2 < userCounts) {	// 其实这里的value没有实际意义，因为在reducer中并没有使用
				context.write(new Text(userID + "&" + username), new Text(key.toString() + "$" + userScore + ";"));
				userIndex2++;
			}
		}
	}

	/**
	 * 
	 * <p>
	 * Title: ColFilterReducer
	 * </p>
	 * <p>
	 * Description: Reducer类
	 * </p>
	 * 
	 * @author Gu Yongtao
	 * @date 2018年12月18日
	 */
	public static class ColFilterReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("----- Start Reducer -----");
			/**
			 * context输出，输出每位用户的推荐电影
			 */
			// 根据userSimilarityMap计算总的相似度
			if (firstRun++ == 0) {
				System.out.println("Calculate all similarity:");
				calculateUserSimilarityAll();
				printfArray(userSimilarityAll);
			}

			// 通过key用户名，查找总的相似度二维数组，通过找出两个最相似的用户（相似度值最小），推荐其电影
			// 分割key为 ：用户下标 用户名
			String idUser = key.toString();
			int index = idUser.indexOf("&");
			int id = Integer.parseInt(idUser.substring(0, index));
			String user = idUser.substring(index + 1);

			// 查找相似用户数
			int similarityCounts = (userCounts*1)/4;	// 用户总数的1/4
			// 第一行表示用户ID，第二行表示用户姓名
			String[][] similarityUsers = new String[2][similarityCounts];
			for (int i = 0; i < similarityCounts; i++) {
				Double temp = 0.0;
				// 获取相似用户下标，并通过下标获取用户名
				for (int j = 0; j < userCounts; j++) {
					// 因为userSimilarityAll的行列顺序与users相对应, 所以 j = users中的用户ID
					// 第一个用户
					if (i == 0) {
						if (userSimilarityAll[id][j] >= temp) {
							// 第一行用户ID
							similarityUsers[0][i] = "" + j;
							// 第二行用户姓名
							similarityUsers[1][i] = getSimilarityUserName(j);
							temp = userSimilarityAll[id][j];
						}
					}
					// 第二个及以后的用户
					/*
					 * 判断条件是为了避免多种状况
					 * 条件1,2是避免出现相同最小值但没有取到
					 * 条件3是避免出现相同值时取到同一用户
					 */
					else if (userSimilarityAll[id][j] >= temp
							&& userSimilarityAll[id][j] <= userSimilarityAll[id][Integer.parseInt(similarityUsers[0][i-1])]
							&& j != Integer.parseInt(similarityUsers[0][i-1])) {
						// 第一行用户ID
						similarityUsers[0][i] = "" + j;
						// 第二行用户姓名
						similarityUsers[1][i] = getSimilarityUserName(j);
						temp = userSimilarityAll[id][j];
					}
				}
			}
			// 控制台输出信息
			System.out.print("User:  " + user + "  <===>  Similarity user: ");
			// 获取相似用户对所有电影的评分
			// key=用户ID，value=mape类型的电影名，评分
			Map<Integer, Map<String, String>> simiUserMovieScoreList = new HashMap<Integer, Map<String,String>>();
			Map<String, String> userMovieScoreMap = null;
			// 便利相似用户数组，获取其对所有电影的评分
			for (int i = 0; i < similarityCounts; i++) {
				System.out.print(similarityUsers[1][i] +", ");	// 控制台输出
				userMovieScoreMap = usersInfo.get(similarityUsers[1][i]);	// 电影-评分
				simiUserMovieScoreList.put(Integer.parseInt(similarityUsers[0][i]), userMovieScoreMap);	// key=用户ID，value=电影-评分
			}
			System.out.println("");
			
			// 根据相似用户，将其电影评分信息计算，计算出推荐电影
			// 按电影名计算推荐值
			// 电影推荐值一位数组
			Double[] movieRecommendScore = new Double[movieCounts];
			// 初始化
			for (int i = 0; i < movieCounts; i++) {
				movieRecommendScore[i] = 0.0;
			}
			// 所有电影对该用户的电影推荐总值
			Double recommendSum = 0.0;

			for (int i = 0; i < movieCounts; i++) {
				// 该电影推荐值= 最相似用户的电影评分*与该用户的总相似度 + 第二相似用户的电影评分*与该用户的总相似度 + ......
				for (Integer oneOfSimilarityID : simiUserMovieScoreList.keySet()) {
					movieRecommendScore[i] += (Double.parseDouble(simiUserMovieScoreList.get(oneOfSimilarityID).get(movies[i]))
							* userSimilarityAll[id][oneOfSimilarityID]);
				}
				// 所有电影对该用户的推荐总值
				recommendSum += movieRecommendScore[i];
			}
			
			// 所有电影推荐度平均值
			Double aveRecommendScore = recommendSum / movieCounts;

			// 属于推荐电影的字符串
			String recommendMovies = "";
			// 遍历电影列表，并判断是否属于推荐电影
			DecimalFormat df = new DecimalFormat("#.00");	// 格式化保留两位小数
			// 推荐电影 key=电影, value=推荐值
			Map<String, String> movieRecomm = new HashMap<>();
			for (int i = 0; i < movieCounts; i++) {
				// 推荐标准：1 推荐度大于平均值； 2 该用户并未看过该电影（即评分为0.0）
				if (movieRecommendScore[i] >= aveRecommendScore && usersInfo.get(user).get(movies[i]).equals("0.0")) {
					// 符合推荐标准
					movieRecomm.put(movies[i], df.format(movieRecommendScore[i]));
				}
			}
			movieRecomm = sortByValueDescending(movieRecomm);
			// 格式：电影名[推荐值]
			for (Entry<String, String> mr : movieRecomm.entrySet()) {
				recommendMovies += mr.getKey() +"[" + mr.getValue() + "]" + "; ";
			}
			// 安照 key=用户，推荐电影：电影1; 电影2; ...输出
			System.out.println("Recommend movie: " + recommendMovies);
			context.write(new Text(user + "\t"), new Text(recommendMovies));
			System.out.println("-------------------------");
		}

		/**
		 * 
		 * @Title：getSimilarityUserName
		 * @Description: 通过用户下标获取用户名
		 * @Param: @param similarityUserId1
		 * @Return: void
		 * @Date: 2018年12月19日
		 */
		private String getSimilarityUserName(int userId) {
			return users[userId];
		}
		
		/**
		 * 
		 * @Title：calculateUserSimilarityAll
		 * @Description: 计算用户的总相似度：将对各电影的评分的差值（5-差值-越大表示二者越相似）之和
		 * @Param: 
		 * @Return: void
		 */
		public void calculateUserSimilarityAll() {
			// 总相似度值越小，二者越相似
			// 总相似度赋值1000
			// 计算总
			userSimilarityAll = new Double[userCounts][userCounts];
			for (String movie : userSimilarityMap.keySet()) {
				// 创建每部电影各用户之间的相似度二维数组，并初始化
				initArray(userSimilarityAll, 0.0);
				// 计算
				for (int i = 0; i < userCounts; i++) {
					for (int j = 0; j < userCounts; j++) {
						userSimilarityAll[i][j] += userSimilarityMap.get(movie)[i][j];
					}
				}
			}
		}
		
		/**
		 * 
		 * @Title：sortByValueDescending
		 * @Description: 按照Map的value值，对其降序排序
		 * @Param: @param map
		 * @Param: @return
		 * @Return: Map<K,V>
		 */
	    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValueDescending(Map<K, V> map)
	    {
	        List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
	        Collections.sort(list, new Comparator<Map.Entry<K, V>>()
	        {
	            @Override
	            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2)
	            {
	                int compare = (o1.getValue()).compareTo(o2.getValue());
	                return -compare;
	            }
	        });

	        Map<K, V> result = new LinkedHashMap<K, V>();
	        for (Map.Entry<K, V> entry : list) {
	            result.put(entry.getKey(), entry.getValue());
	        }
	        return result;
	    }
	}

	/**
	 * 
	 * @Title：initArray
	 * @Description: 初始化二维数组
	 * @Param: @param similarity
	 * @Return: void
	 * @Date: 2018年12月19日
	 */
	public static void initArray(Double[][] similarity, Double initNum) {
		for (int i = 0; i < userCounts; i++) {
			for (int j = 0; j < userCounts; j++) {
				similarity[i][j] = initNum;
			}
		}
	}
	
	/**
	 * 
	 * @Title：printfArray
	 * @Description: 输出二位数组的值
	 * @Param: @param userSimilarityAll
	 * @Return: void
	 */
	private static void printfArray(Double[][] userSimilarityAll) {
		for (int i = 0; i < userCounts; i++) {
			for (int j = 0; j < userCounts; j++) {
				System.out.print(userSimilarityAll[i][j] + "    ");
			}
			System.out.println("");
		}
	}
	
}
