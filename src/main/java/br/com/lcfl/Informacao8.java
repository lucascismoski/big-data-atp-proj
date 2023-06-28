package br.com.lcfl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Informacao8 {
    public static class MapperInformacao8 extends Mapper<Object, Text, Text, FloatWritable> {
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
            String linha = valor.toString();
            String[] campos = linha.split(";");
            if (campos.length == 10){
                String mercadoriaConcatenadaComAno = campos[3]+'_'+campos[1];
                float peso = Float.valueOf(campos[6]);

                Text chaveMap = new Text(mercadoriaConcatenadaComAno);
                FloatWritable valorMap = new FloatWritable(peso);

                context.write(chaveMap, valorMap);
            }
        }
    }

    public static class ReducerInformacao8 extends Reducer<Text, FloatWritable, Text, FloatWritable> {
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
        Job job = Job.getInstance(conf, "informacao8");

        job.setJarByClass(Informacao8.class);
        job.setMapperClass(MapperInformacao8.class);
        job.setReducerClass(ReducerInformacao8.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));

        job.waitForCompletion(true);
    }
}
