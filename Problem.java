/*
Data Intensive Computing
Project2 Problem1



Amol Salunkhe- aas22@buffalo.edu - 29612314

Mihir Kulkarni- mihirdha@buffalo.edu - 50168610

*/
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Problem {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable instanceCount = new IntWritable(1);
    private Text roomSemesterKey = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	//SemesterId, SemesterName,Location,DayOfWeek,ClassTime,CourseNo,CourseName, NoOfStudents,MaxStudentsAllowed	
      StringTokenizer itr = new StringTokenizer(value.toString(),",");
      
      int numOfTokens = itr.countTokens();
	  
      int currTokenNum =0;
	  String semesterName ="",location="",capacity="",building="";
	  boolean doNotProcess = false;
	  
	  while (itr.hasMoreTokens()) {
        currTokenNum++;
		if(currTokenNum == 2){
			semesterName = itr.nextToken();
		}else if(currTokenNum == 3){
			location = itr.nextToken();
			//since the location is unknown do not processes it
			StringTokenizer itr2 = new StringTokenizer(location," ");
			building=itr2.nextToken();
			if (building.equals("Unknown")||building.equals("Arr")||building.equals("ARR")||building.equals(null)){
				doNotProcess = true;
				break;
			}
		}
		else if (numOfTokens-1 == currTokenNum){//else if (numOfTokens-1 == currTokenNum)
		//else if (currTokenNum==9){//else if (numOfTokens-1 == currTokenNum)
			capacity = itr.nextToken();
		}
		else{
			itr.nextToken();
		}
		//if(numOfTokens>9) doNotProcess=true;//TODO Do we need this?
	}//while (itr.hasMoreTokens())
	  if(!doNotProcess){
		roomSemesterKey.set(building+"_"+semesterName);
		//TODO:convert capacity to IntWritable
		//context.write(roomSemesterKey, new IntWritable(Integer.parseInt(capacity)));
		context.write(roomSemesterKey, new IntWritable(Integer.parseInt(capacity)));
		
	  }//if(!doNotProcess)
    }//public void map
  }//public static class TokenizerMapper

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Problem.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
