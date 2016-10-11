package order;

public interface IPipe {
    public void write(Order s);
    public Order read();    
    public void close();
}