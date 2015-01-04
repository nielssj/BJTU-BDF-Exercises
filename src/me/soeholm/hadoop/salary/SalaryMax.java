package me.soeholm.hadoop.salary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by niels on 12/22/14.
 */

public class SalaryMax {

    /*
        (Writable) Employee class

        Introduced to be able to return both name and salary as
        a result of map and reduce operations.
     */
    public static class Employee implements Writable {
        private Text name;
        private IntWritable salary;

        public Employee() {
            this.name = new Text();
            this.salary = new IntWritable();
        }

        public Employee(Employee employee) {
            this.name = new Text();
            this.name.set(employee.getName().toString());
            this.salary = new IntWritable();
            this.salary.set(employee.getSalary().get());
        }

        public void setName(String name) {
            this.name.set(name);
        }

        public void setSalary(int salary) {
            this.salary.set(salary);
        }

        public Text getName() {
            return name;
        }

        public IntWritable getSalary() {
            return salary;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            name.write(dataOutput);
            salary.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            name.readFields(dataInput);
            salary.readFields(dataInput);
        }

        @Override
        public String toString() {
            return String.format("%s (%d)",
                    this.getName().toString(),
                    this.getSalary().get());
        }
    }

    // Map(lineOfData) -> (department, (name, salary)
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Employee> {
        private Text department = new Text();
        private Employee employee = new Employee();

        public TokenizerMapper() {
            super();
        }

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");

            // Department
            department.set(parts[7]);

            // Employee
            String name = parts[1];
            int salary = Integer.parseInt(parts[5]);
            int bonus = 0;
            if(!parts[6].equals("")) {
                bonus = Integer.parseInt(parts[6]);
            }
            employee.setName(name);
            employee.setSalary(salary + bonus);

            context.write(department, employee);
        }
    }

    // Reduce(department, [(name, salary), (name, salary), ...]) -> (department, (name, salary))
    public static class IntSumReducer extends Reducer<Text, Employee, Text, Employee> {
        public void reduce(Text key, Iterable<Employee> values, Context context) throws IOException, InterruptedException {
            Employee highestEmployee = new Employee();
            int highestVal = 0;

            for (Employee employee : values) {
                int salary = employee.getSalary().get();
                if(salary > highestVal) {
                    highestEmployee = new Employee(employee);
                    highestVal = salary;
                }
            }

            context.write(key, highestEmployee);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "salary max");
        job.setJarByClass(SalarySum.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Employee.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Employee.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}