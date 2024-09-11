package questoes.q5;

public class averageWritable {
    Long total;
    Long quantity;

    public averageWritable (Long total, Long quantity) {
        this.setTotal(total);
        this.setQuantity(quantity);
    }
    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public Long getQuantity() {
        return quantity;
    }

    public void setQuantity(Long quantity) {
        this.quantity = quantity;
    }
}
