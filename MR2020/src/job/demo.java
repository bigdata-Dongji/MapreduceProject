package job;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class demo {

    public static void main(String[] args) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = sdf.parse("2020-10-13");
        Calendar cld = Calendar.getInstance();

        cld.setTime(date);
//        String mm = new SimpleDateFormat("MM").format(date);
        cld.set(Calendar.DATE, cld.get(Calendar.DATE)-1);
        System.out.println(sdf.format(cld.getTime()));
//        Date datas=new Date();
//        System.out.println(sdf.format(datas));
//        String word="1.5亿";
//        String words="1.5万";
//        if(word.contains("亿")){
//            String num_one=word.substring(0,word.indexOf("亿"));
//            double piao_one=Float.parseFloat(num_one)*10000.0;
//            System.out.println(piao_one);
//        }
//        System.out.println(word.indexOf("亿"));
//        if(words.contains("万")){
//            String num_one_day=words.substring(0,words.indexOf("万"));
//            double piao_one_day=Float.parseFloat(num_one_day);
//            System.out.println(piao_one_day);
//            System.out.println("sdad"+piao_one_day);
//        }
//        else{
//            return;
//        }
    }
}
