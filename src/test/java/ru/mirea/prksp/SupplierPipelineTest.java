package ru.mirea.prksp;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SupplierPipelineTest {
    @Test
    void continuesAfterBadRecordAndUpdatesSupplierTotals() {
        List<Throwable> errors = new ArrayList<>();
        TestSubscriber<SupplierTotal> subscriber = SupplierPipeline.totals(Flowable.just(
                offer(1, "Альфа", "Молоко", "10.00", 2),
                offer(2, "Бета", "Хлеб", "5.00", 3),
                offer(1, "Альфа", "Нет в наличии", "3.00", 0),
                offer(1, "Альфа", "Ошибка", "-1.00", 2),
                offer(1, "Альфа", "Сыр", "2.50", 4)), errors::add).test();

        subscriber.assertComplete().assertNoErrors().assertValueCount(3);
        assertEquals(1, errors.size());
        assertEquals(0, new BigDecimal("30.00").compareTo(subscriber.values().get(2).totalCost()));
        assertEquals(6, subscriber.values().get(2).availableQuantity());
        assertEquals(1, subscriber.values().get(2).supplierId());
        assertEquals(0, new BigDecimal("15.00").compareTo(subscriber.values().get(1).totalCost()));
    }

    private static SupplierOffer offer(int id, String supplier, String product, String price, int quantity) {
        return new SupplierOffer(id, supplier, product, new BigDecimal(price), quantity);
    }
}
