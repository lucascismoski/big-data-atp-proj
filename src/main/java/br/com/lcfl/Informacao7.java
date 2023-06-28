package br.com.lcfl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Informacao7 {
    public static class MapperInformacao7 extends Mapper<Object, Text, Text, FloatWritable> {
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
            String linha = valor.toString();
            String[] campos = linha.split(";");
            if (campos.length == 10){
                String mercadoria = campos[3];

                float peso;
                try {
                    peso = Float.valueOf(campos[6]);
                } catch (Exception e) {
                    peso = 0;
                }

                Text chaveMap = new Text(mercadoria);
                FloatWritable valorMap = new FloatWritable(peso);

                context.write(chaveMap, valorMap);
            }
        }
    }

    public static class ReducerInformacao7 extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        @Override
        public void reduce(Text chave, Iterable<FloatWritable> valores, Context context) throws IOException, InterruptedException {
            float soma = 0;
            for(FloatWritable val : valores){
                soma += val.get();
            }
            FloatWritable valorSaida = new FloatWritable(soma);
            context.write(chave, valorSaida);
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String arquivoEntrada = "C:\\Users\\lucas\\OneDrive\\Documentos\\dev\\BigData\\base_inteira.csv";
        String arquivoSaida = "C:\\Users\\lucas\\OneDrive\\Documentos\\dev\\BigData\\BigDataResults\\implementacao1";

        if(args.length == 2){
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "informacao7");

        job.setJarByClass(Informacao7.class);
        job.setMapperClass(MapperInformacao7.class);
        job.setReducerClass(ReducerInformacao7.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));

        job.waitForCompletion(true);
    }
}
