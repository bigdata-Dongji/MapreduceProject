package meteorological;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**封装对象*/
public class Weather implements WritableComparable<Weather> {
    //年
    private String year;
    //月
    private String month;
    //日
    private String day;
    //小时
    private String hour;
    //温度
    private int temperature;
    //湿度
    private String dew;
    //气压/压强
    private int pressure;
    //风向
    private String wind_direction;
    //风速
    private int wind_speed;
    //天气情况
    private String sky_condition;
    //1小时降雨量
    private String rain_1h;
    //6小时降雨量
    private String rain_6h;

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    public String getDew() {
        return dew;
    }

    public void setDew(String dew) {
        this.dew = dew;
    }

    public int getPressure() {
        return pressure;
    }

    public void setPressure(int pressure) {
        this.pressure = pressure;
    }

    public String getWind_direction() {
        return wind_direction;
    }

    public void setWind_direction(String wind_direction) {
        this.wind_direction = wind_direction;
    }

    public int getWind_speed() {
        return wind_speed;
    }

    public void setWind_speed(int wind_speed) {
        this.wind_speed = wind_speed;
    }

    public String getSky_condition() {
        return sky_condition;
    }

    public void setSky_condition(String sky_condition) {
        this.sky_condition = sky_condition;
    }

    public String getRain_1h() {
        return rain_1h;
    }

    public void setRain_1h(String rain_1h) {
        this.rain_1h = rain_1h;
    }

    public String getRain_6h() {
        return rain_6h;
    }

    public void setRain_6h(String rain_6h) {
        this.rain_6h = rain_6h;
    }


    public Weather() {

    }

    public Weather(String year, String month, String day, String hour, int temperature, String dew, int pressure,
                           String wind_direction, int wind_speed, String sky_condition, String rain_1h, String rain_6h) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.temperature = temperature;
        this.dew = dew;
        this.pressure = pressure;
        this.wind_direction = wind_direction;
        this.wind_speed = wind_speed;
        this.sky_condition = sky_condition;
        this.rain_1h = rain_1h;
        this.rain_6h = rain_6h;

    }

    public void readFields(DataInput in) throws IOException {
        year = in.readUTF();
        month = in.readUTF();
        day = in.readUTF();
        hour = in.readUTF();
        temperature = in.readInt();
        dew = in.readUTF();
        pressure = in.readInt();
        wind_direction = in.readUTF();
        wind_speed = in.readInt();
        sky_condition = in.readUTF();
        rain_1h = in.readUTF();
        rain_6h = in.readUTF();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(year);
        out.writeUTF(month);
        out.writeUTF(day);
        out.writeUTF(hour);
        out.writeInt(temperature);
        out.writeUTF(dew);
        out.writeInt(pressure);
        out.writeUTF(wind_direction);
        out.writeInt(wind_speed);
        out.writeUTF(sky_condition);
        out.writeUTF(rain_1h);
        out.writeUTF(rain_6h);
    }

    @Override
    public String toString() {
        return year + "," + month + "," + day + "," + hour + "," + temperature + "," + dew + "," + pressure +
                "," + wind_direction + "," + wind_speed + "," + sky_condition + "," + rain_1h + "," + rain_6h ;
    }

    /*
    * 对进入同一个分区的数据排序； 排序规则： （1）同年同月同天为key； （2）按每日温度升序；
    * （3）若温度相同则按风速升序； （4）风速相同则按压强降序。
    * */
    public int compareTo(Weather o) {
        int tmp=this.month.compareTo(o.month);
        if (tmp==0){
            tmp=this.day.compareTo(o.day);
            if (tmp==0){
                tmp=this.temperature-o.temperature;
                if (tmp==0){
                    tmp=this.wind_speed-o.wind_speed;
                    if (tmp==0){
                        tmp=this.pressure-o.pressure;
                        return tmp;
                    }return tmp;
                }return tmp;
            }return tmp;
        }return tmp;
    }

}

