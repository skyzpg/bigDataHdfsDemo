package cn.itcast.test1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWritable implements WritableComparable<PairWritable>{

    // 组合key,第一部分是我们第一列，第二部分是我们第二列
    private String  word;
    private int num;

    public PairWritable() {
        this.set(word,num );
    }

    public void set(String word, int num){
        this.word = word;
        this.num = num;
    }
    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    //重写比较器
    //每次比较都是调用该方法的对象与传递的参数进行比较，说白了就是第一行与第二行比较完了之后的结果与第三行比较，
    //得出来的结果再去与第四行比较，依次类推
    @Override
    public int compareTo(PairWritable o) {
        int comp = this.word.compareTo(o.word);
        if (comp != 0) {
            return comp;
        } else { // 若第一个字段相等，则比较第二个字段
            return Integer.valueOf(this.num).compareTo(Integer.valueOf(o.num));
        }
    }

    @Override
    public String toString() {
        return "PairWritable{" +
                "word='" + word + '\'' +
                ", num=" + num +
                '}';
    }

    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeInt(num);


    }
    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word = dataInput.readUTF();
        this.num = dataInput.readInt();
    }


}
