import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GraphNode implements Writable {
    private IntWritable id;
    private Text url;
    private Text outLinks;
    private Text inLinks;
    private BooleanWritable isGraphNode;
    private FloatWritable pageRank;

    public GraphNode(){
        id = new IntWritable();
        url = new Text();
        outLinks = new Text();
        isGraphNode = new BooleanWritable();
        pageRank = new FloatWritable();
    }

    public GraphNode(int id, String url, Boolean isGraphNode, float pageRank)
    {
        this.id = new IntWritable(id);
        this.url = new Text(url);
        this.isGraphNode = new BooleanWritable(isGraphNode);
        this.pageRank = new FloatWritable(pageRank);
    }


    public void write(DataOutput out) throws IOException {
        id.write(out);
        getUrl().write(out);
        outLinks.write(out);
        isGraphNode.write(out);
        pageRank.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        getUrl().readFields(in);
        outLinks.readFields(in);
        isGraphNode.readFields(in);
        pageRank.readFields(in);
    }

    public int compareTo(GraphNode o) {
        return url.compareTo(o.url);
    }

    public String[] getOutLinks() {
        if (outLinks.toString().equals("[]"))
            return null;
        return outLinks.toString().replace("[", "").replace("]", "").split(",");
    }

    public String getRawOutLinks() {
        return outLinks.toString();
    }

    public void setOutLinks(String outLinks) {
        this.outLinks = new Text(outLinks.replace(", ", ","));
    }

    public Text getUrl() {
        return url;
    }

    public FloatWritable getPageRank() {
        return pageRank;
    }

    public void setPageRank(FloatWritable pageRank) {
        this.pageRank = pageRank;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(id);
        sb.append("\t");
        sb.append(url);
        sb.append("\t");
        sb.append(outLinks);
        sb.append("\t");
        sb.append(inLinks);
        sb.append("\t");
        sb.append(isGraphNode);
        sb.append("\t");
        sb.append(pageRank);
        return sb.toString();
    }

    public BooleanWritable getIsGraphNode() {
        return isGraphNode;
    }

    public void setIsGraphNode(BooleanWritable isGraphNode) {
        this.isGraphNode = isGraphNode;
    }

    public IntWritable getId() {
        return id;
    }

    public void setId(IntWritable id) {
        this.id = id;
    }

    public void setUrl(Text url) {
        this.url = url;
    }
}
