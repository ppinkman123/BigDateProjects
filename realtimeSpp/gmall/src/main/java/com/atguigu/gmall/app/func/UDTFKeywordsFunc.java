package com.atguigu.gmall.app.func;

import jdk.nashorn.internal.objects.annotations.Function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<keyword STRING>"))
public class UDTFKeywordsFunc extends TableFunction<Row> {

//     *   public void eval(Integer... args) {
// *     for (Integer i : args) {
// *       collect(i);
    public void eval(String str){
        List<String> strings = flatWords(str);
        for (String string : strings) {
            collect(Row.of(string));
        }
    }

    public List<String> flatWords(String string){

        List<String> res = new ArrayList<>();

        StringReader stringReader = new StringReader(string);

        IKSegmenter ikSegmenter = new IKSegmenter(stringReader,true);

        //这样每次调用这个方法的时候，next都是空的。
        Lexeme next = null;

        try {
        while ((next = ikSegmenter.next())!=null){
           res.add(next.getLexemeText());
        }
        }catch (Exception e){
            e.printStackTrace();
        }

        return res;
    }
}
