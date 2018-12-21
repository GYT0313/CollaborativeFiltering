package com.gyt.collaborativeFiltering;

import java.util.*;

/**
 * 描述：对电影打星1~5，最低打1星，0代表没打星过。大于平均推荐度代表喜欢。
 * 给目标用户推荐相似度最高用户喜欢的电影
 */
public class UserCFDemo {

    //系统用户
    private static String[] users={"小明","小花","小美","小张","小李"};
    //和这些用户相关的电影
    private static String[] movies={"电影1","电影2","电影3","电影4","电影5","电影6","电影7"};
    //用户点评电影打星数据,是users对应用户针对movies对应电影的评分
    private static int[][] allUserMovieStarList={
            {3,1,4,4,1,0,0},
            {0,5,1,0,0,4,0},
            {1,0,5,4,3,5,2},
            {3,1,4,3,5,0,0},
            {5,2,0,1,0,5,5}
    };
    //相似用户集合
    private static List<List<Object>> similarityUsers=null;
    //推荐所有电影集合
    private static List<String> targetRecommendMovies=null;
    //点评过电影集合
    private static List<String> commentedMovies=null;
    //用户在电影打星集合中的位置
    private static Integer targetUserIndex=null;

    public static void main(String[] args) {
        Scanner scanner=new Scanner(System.in);
        String user=scanner.nextLine();
        while (user!=null && !"exit".equals(user)){
            targetUserIndex=getUserIndex(user);	// 计算用户位置
            if(targetUserIndex==null){
                System.out.println("没有搜索到此用户，请重新输入：");
            }else{
                //计算用户相似度
                calcUserSimilarity();
                
                //计算电影推荐度，排序
                calcRecommendMovie();
                
                //处理推荐电影列表
                handleRecommendMovies();	// 将已评分的电影加入列表
                
                //输出推荐电影
                System.out.print("推荐电影列表：");
                for (String item:targetRecommendMovies){
                    if(!commentedMovies.contains(item)){
                        System.out.print(item+"  ");
                    }
                }
                System.out.println();
            }

            user=scanner.nextLine();
            targetRecommendMovies=null;
        }

    }

    /**
     * 把推荐列表中用户已经点评过的电影剔除
     */
    private static void handleRecommendMovies(){
        commentedMovies=new ArrayList<>();
        for (int i=0;i<allUserMovieStarList[targetUserIndex].length;i++){
            if(allUserMovieStarList[targetUserIndex][i]!=0){
                commentedMovies.add(movies[i]);
            }
        }
    }


    /**
     * 获取全部推荐电影,计算平均电影推荐度
     */
    private static void calcRecommendMovie(){
        /*
         * -计算每部电影对目标用户的推荐度
         * 
         * -用户下标: Integer.parseInt(similarityUsers.get(0).get(0).toString())
         * -i 电影下标
         * 
         * recommdRate： 每部电影的评分推荐度
         * recommdRate = 相似度最高的用户的第i部电影的评分*相似度   +   相似度第二高的用户的第i部电影的评分*相似度
         * 
         * recommendMovie: [0] 电影下标， [1] 推荐度
         */
        targetRecommendMovies=new ArrayList<>();
        List<List<Object>> recommendMovies=new ArrayList<>();
        List<Object> recommendMovie=null;
        double recommdRate=0,sumRate=0;
        
        for (int i=0;i<7;i++){
            recommendMovie=new ArrayList<>();
            recommendMovie.add(i);

            recommdRate=allUserMovieStarList[Integer.parseInt(similarityUsers.get(0).get(0).toString())][i]*Double.parseDouble(similarityUsers.get(0).get(1).toString())
                    +allUserMovieStarList[Integer.parseInt(similarityUsers.get(1).get(0).toString())][i]*Double.parseDouble(similarityUsers.get(1).get(1).toString());
            recommendMovie.add(recommdRate);
            recommendMovies.add(recommendMovie);
            sumRate+=recommdRate;
        }

        sortCollection(recommendMovies,-1);

        for (List<Object> item:recommendMovies){
            if(Double.parseDouble(item.get(1).toString()) > sumRate/7){ //大于平均推荐度的商品才有可能被推荐
                targetRecommendMovies.add(movies[Integer.parseInt(item.get(0).toString())]);
            }
        }
    }

    /**
     * 获取两个最相似的用户
     */
    private static void calcUserSimilarity(){
        similarityUsers=new ArrayList<>();	// 相似用户集合
        List<List<Object>> userSimilaritys=new ArrayList<>();
        for (int i=0;i<5;i++){	// 计算目标用户与每个用户的相似度
            if(i==targetUserIndex){	// 用户本身跳过
                continue;
            }
            List<Object> userSimilarity=new ArrayList<>();
            userSimilarity.add(i);
            userSimilarity.add(calcTwoUserSimilarity(allUserMovieStarList[i],allUserMovieStarList[targetUserIndex]));	// 根据打分计算相似度
            userSimilaritys.add(userSimilarity);
        }

        sortCollection(userSimilaritys,1);	// 按照降序，将用户相似度列表排序

        similarityUsers.add(userSimilaritys.get(0));	// 应该是推荐两个相似度最高的
        similarityUsers.add(userSimilaritys.get(1));
    }

    /**
     * 根据用户数据，计算用户相似度
     * @param user1Stars
     * @param user2Starts
     * @return
     */
    private static double calcTwoUserSimilarity(int[] user1Stars,int[] user2Starts){
        float sum=0;
        for(int i=0;i<7;i++){	// 计算每部电影的打分之差
            sum += Math.pow(user1Stars[i]-user2Starts[i],2);
        }
        return Math.sqrt(sum);	// 返回所有评分差值的和
    }

    /**
     * 查找用户所在的位置
     * @param user
     * @return
     */
    private static Integer getUserIndex(String user){
        if(user==null || "".contains(user)){
            return null;
        }

        for(int i=0;i<users.length;i++){
            if(user.equals(users[i])){
                return i;
            }
        }

        return null;
    }

    /**
     * 集合排序
     * @param list
     * @param order  1正序 -1倒序
     */
    private static void sortCollection(List<List<Object>> list,int order){
        Collections.sort(list, new Comparator<List<Object>>() {
            @Override
            public int compare(List<Object> o1, List<Object> o2) {
                if(Double.valueOf(o1.get(1).toString()) > Double.valueOf(o2.get(1).toString())){
                    return order;
                }else if(Double.valueOf(o1.get(1).toString()) < Double.valueOf(o2.get(1).toString())){
                    return -order;
                }else{
                    return 0;
                }
            }
        });
    }
}























