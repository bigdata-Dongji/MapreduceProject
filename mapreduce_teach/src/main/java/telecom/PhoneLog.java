package telecom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**封装对象类*/
public class PhoneLog implements WritableComparable<PhoneLog> {

    private String userA;
    private String userB;
    private String userA_Phone;
    private String userB_Phone;
    private String startTime;
    private String endTime;
    private Long timeLen;
    private String userA_Address;
    private String userB_Address;

    public PhoneLog() {

    }

    public void SetPhoneLog(String userA, String userB, String userA_Phone, String userB_Phone, String startTime,
                            String endTime, Long timeLen, String userA_Address, String userB_Address) {
        this.userA = userA;
        this.userB = userB;
        this.userA_Phone = userA_Phone;
        this.userB_Phone = userB_Phone;
        this.startTime = startTime;
        this.endTime = endTime;
        this.timeLen = timeLen;
        this.userA_Address = userA_Address;
        this.userB_Address = userB_Address;
    }

    public String getUserA_Phone() {
        return userA_Phone;
    }

    public void setUserA_Phone(String userA_Phone) {
        this.userA_Phone = userA_Phone;
    }

    public String getUserB_Phone() {
        return userB_Phone;
    }

    public void setUserB_Phone(String userB_Phone) {
        this.userB_Phone = userB_Phone;
    }

    public String getUserA() {
        return userA;
    }

    public void setUserA(String userA) {
        this.userA = userA;
    }

    public String getUserB() {
        return userB;
    }

    public void setUserB(String userB) {
        this.userB = userB;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public Long getTimeLen() {
        return timeLen;
    }

    public void setTimeLen(Long timeLen) {
        this.timeLen = timeLen;
    }

    public String getUserA_Address() {
        return userA_Address;
    }

    public void setUserA_Address(String userA_Address) {
        this.userA_Address = userA_Address;
    }

    public String getUserB_Address() {
        return userB_Address;
    }

    public void setUserB_Address(String userB_Address) {
        this.userB_Address = userB_Address;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(userA);
        out.writeUTF(userB);
        out.writeUTF(userA_Phone);
        out.writeUTF(userB_Phone);
        out.writeUTF(startTime);
        out.writeUTF(endTime);
        out.writeLong(timeLen);
        out.writeUTF(userA_Address);
        out.writeUTF(userB_Address);
    }

    public void readFields(DataInput in) throws IOException {
        userA = in.readUTF();
        userB = in.readUTF();
        userA_Phone = in.readUTF();
        userB_Phone = in.readUTF();
        startTime = in.readUTF();
        endTime = in.readUTF();
        timeLen = in.readLong();
        userA_Address = in.readUTF();
        userB_Address = in.readUTF();
    }
    @Override
    public String toString() {
        return userA + "," + userB + "," + userA_Phone + "," + userB_Phone + "," + startTime + "," + endTime + ","
                + timeLen + "," + userA_Address + "," + userB_Address;
    }

    public int compareTo(PhoneLog pl) {
        if(this.hashCode() == pl.hashCode()) {
            return 0;
        }
        return -1;
    }

}

