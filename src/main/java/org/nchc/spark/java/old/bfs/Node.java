package org.nchc.spark.java.old.bfs;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ogre on 2015/5/2.
 */
public class Node implements Serializable{

    public static enum Color {
        WHITE, GRAY, BLACK
    };

    private int id;
    private int distance;
    private List<Integer> edges = new ArrayList<Integer>();
    private Color color = Color.WHITE;

    public Node(String str) {

        String[] map = str.split("\t");
        String key = map[0];
        String value = map[1];

        String[] tokens = value.split("\\|");

        this.id = Integer.parseInt(key);

        for (String s : tokens[0].split(",")) {
            if (s.length() > 0) {
                edges.add(Integer.parseInt(s));
            }
        }

        if (tokens[1].equals("Integer.MAX_VALUE")) {
            this.distance = Integer.MAX_VALUE;
        } else {
            this.distance = Integer.parseInt(tokens[1]);
        }

        this.color = Color.valueOf(tokens[2]);

    }

    public Node(int id) {
        this.id = id;
    }

    public Node(){
        this.id = -1;
        this.distance = Integer.MAX_VALUE;
    }

    public Node fold(Node other){
        setId(other.getId());

        if (other.getEdges().size() > 0) {
            edges = other.getEdges();
        }

        if (other.getDistance() < distance) {
            distance = other.getDistance();
        }

        // Save the darkest color
        if (other.getColor().ordinal() > color.ordinal()) {
            color = other.getColor();
        }
        return this;
    }

    public void setId(int id ){this.id = id;}

    public int getId() {
        return this.id;
    }

    public int getDistance() {
        return this.distance;
    }

    public void setDistance(int distance) {
        this.distance = distance;
    }

    public Color getColor() {
        return this.color;
    }

    public void setColor(Color color) {
        this.color = color;
    }

    public List<Integer> getEdges() {
        return this.edges;
    }

    public void setEdges(List<Integer> edges) {
        this.edges = edges;
    }

    public String getLine() {
        StringBuffer s = new StringBuffer();
        //s.append(id).append("\t");
        for (int v : edges) {
            s.append(v).append(",");
        }
        s.append("|");

        if (this.distance < Integer.MAX_VALUE) {
            s.append(this.distance).append("|");
        } else {
            s.append("Integer.MAX_VALUE").append("|");
        }

        s.append(color.toString());

        return s.toString();
    }

    public String toString(){
        return id+"\t"+getLine();
    }
}
