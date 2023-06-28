package br.com.lcfl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Implementacao1 {
    public static class MapperImplementacao1 extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
            String linha = valor.toString();
            String[] campos = linha.split(";");
            if (campos.length == 10){
                String pais = campos[0];
                int ocorrencia = 1;

                Text chaveMap = new Text(pais);
                IntWritable valorMap = new IntWritable(ocorrencia);

                context.write(chaveMap, valorMap);
            }
        }
    }

    public static class ReducerImplementacao1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException {
            int soma = 0;
            for(IntWritable val : valores){
                soma += val.get();
            }
            IntWritable valorSaida = new IntWritable(soma);
            context.write(chave, valorSaida);
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String arquivoEntrada = "C:\\Users\\lucas\\OneDrive\\Documentos\\dev\\BigData\\base_inteira.csv";
        String arquivoSaida = "C:\\Users\\lucas\\OneDrive\\Documentos\\dev\\BigData\\BigDataResults\\implementacao1";

//        if(args.length == 2){
//            arquivoEntrada = args[0];
//            arquivoSaida = args[1];
//        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "atividade1");

        job.setJarByClass(Implementacao1.class);
        job.setMapperClass(MapperImplementacao1.class);
        job.setReducerClass(ReducerImplementacao1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));

        job.waitForCompletion(true);
    }
}