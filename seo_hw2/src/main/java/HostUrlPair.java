import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HostUrlPair implements WritableComparable<HostUrlPair> {

    private Text host;
    private Text query;

    public HostUrlPair() {
        host = new Text();
        query = new Text();
    }

    public HostUrlPair(Text host, Text query) {
        this.host = host;
        this.query = query;
    }

    public HostUrlPair(String host, String query) {
        this.host = new Text(host);
        this.query = new Text(query);
    }


    public Text getHost() {
        return host;
    }

    public Text getQuery() {
        return query;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        host.write(out);
        query.write(out);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        host.readFields(dataInput);
        query.readFields(dataInput);
    }

    @Override
    public int compareTo(@Nonnull HostUrlPair o) {
        int cmp = host.compareTo(o.host);
        return (cmp == 0) ? query.compareTo(o.query) : cmp;
    }

    @Override
    public int hashCode() {
        return host.hashCode() * 163 + query.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HostUrlPair) {
            HostUrlPair tp = (HostUrlPair) obj;
            return host.equals(tp.host) && query.equals(tp.query);
        }
        return false;
    }

    public String toString() {
        return host + "\t" + query;
    }

}
