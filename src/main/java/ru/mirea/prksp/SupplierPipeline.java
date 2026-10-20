package ru.mirea.prksp;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.functions.Consumer;

import java.math.BigDecimal;
import java.util.Objects;

/** Реактивная обработка поставок. Каждый некорректный элемент изолирован. */
public final class SupplierPipeline {
    private SupplierPipeline() {
    }

    public static Flowable<SupplierTotal> totals(
            Flowable<SupplierOffer> source,
            Consumer<? super Throwable> errorHandler
    ) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(errorHandler, "errorHandler");

        return source
                // Ошибка одной записи не завершает поток следующих поставок.
                .concatMap(offer -> Flowable.fromCallable(() -> validate(offer))
                        .onErrorResumeNext(error -> {
                            errorHandler.accept(error);
                            return Flowable.empty();
                        }))
                .filter(offer -> offer.quantity() > 0)
                .map(offer -> new AvailableProduct(
                        offer.supplierId(),
                        offer.supplierName(),
                        offer.product(),
                        offer.quantity(),
                        offer.price().multiply(BigDecimal.valueOf(offer.quantity()))))
                .groupBy(AvailableProduct::supplierId)
                .flatMap(group -> group
                        .scan(new SupplierTotal(group.getKey(), "", 0, BigDecimal.ZERO),
                                (previous, product) -> new SupplierTotal(
                                        product.supplierId(),
                                        product.supplierName(),
                                        previous.availableQuantity() + product.quantity(),
                                        previous.totalCost().add(product.totalCost())))
                        .skip(1));
    }

    private static SupplierOffer validate(SupplierOffer offer) {
        if (offer.supplierId() <= 0) {
            throw new IllegalArgumentException("supplierId должен быть положительным");
        }
        if (offer.supplierName() == null || offer.supplierName().isBlank()) {
            throw new IllegalArgumentException("Пустое название поставщика: " + offer.supplierId());
        }
        if (offer.product() == null || offer.product().isBlank()) {
            throw new IllegalArgumentException("Пустое название товара: " + offer.supplierId());
        }
        if (offer.price() == null || offer.price().signum() < 0) {
            throw new IllegalArgumentException("Некорректная цена товара " + offer.product());
        }
        if (offer.quantity() < 0) {
            throw new IllegalArgumentException("Отрицательное количество товара " + offer.product());
        }
        return offer;
    }
}
