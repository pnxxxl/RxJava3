package ru.mirea.prksp;

import java.math.BigDecimal;

/** Данные о товаре в поставке. Аналог Good + GoodsList из BootcampLabs. */
public record SupplierOffer(
        int supplierId,
        String supplierName,
        String product,
        BigDecimal price,
        int quantity
) {
}
