package ru.shaldnikita.hadoop.inputFormat;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

@Data
public class URLWritable implements Writable{
    protected URL url;

    public URLWritable(URL url) {
        this.url = url;
    }

    public URLWritable(String url) throws MalformedURLException {
        this.url = new URL(url);
    }

    public URLWritable() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(url.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        url = new URL(dataInput.readUTF());
    }
}
