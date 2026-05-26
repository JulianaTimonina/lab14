#ifndef VALIDATION_H
#define VALIDATION_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    const char* meter_id;
    int64_t timestamp;
    double power;
} CMeterReading;

typedef struct {
    bool is_valid;
    const char* error_message;
} CValidationResult;

// Валидирует одно показание
// Возвращает 0 если успешно, иначе 1 (ошибка вызова)
int validate_meter_reading_c(const CMeterReading* reading, CValidationResult* result);

// Освобождает память, выделенную для сообщения об ошибке
void free_validation_result(CValidationResult* result);

#ifdef __cplusplus
}
#endif

#endif // VALIDATION_H