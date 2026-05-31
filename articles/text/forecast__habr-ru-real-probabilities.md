---
title: "Прогнозируем реальные вероятности (Forecasting Real Probabilities)"
url: https://habr.com/ru/articles/648753/
source: habr
date: "2022-01-31"
type: blog
theme: forecast
lang: ru
---

# Прогнозируем реальные вероятности

**Автор:** NewTechAudit
**Дата:** 31 января 2022
**Хабы:** Python, Машинное обучение

## Основное содержание

Статья посвящена калибровке моделей машинного обучения для получения достоверных вероятностных прогнозов.

## Суть проблемы

Ни одна модель не может абсолютно точно предсказывать реальные вероятности. Необходимо откалибровать модель так, чтобы "полученные показатели распределения вероятностей были как можно ближе к реальным".

## Методы калибровки

1. Гистограммная калибровка
2. Изотоническая регрессия
3. Калибровка Платта (Platt scaling)
4. Логистическая регрессия
5. Деревья калибровки
6. Ансамблирование

## Практическое применение на Python

Демонстрируется использование двух методов на основе RandomForestClassifier:
1. **Логистическая регрессия** — строит прогнозы в диапазоне [0, 1]
2. **Изотоническая регрессия** — подгоняет монотонную кривую к последовательности наблюдений

## Результаты тестирования (Expected Calibration Error — ECE)

| Метод | ECE |
|---|---|
| RandomForest (без калибровки) | 7.0% |
| RandomForest + Логистическая регрессия | 2.3% |
| RandomForest + Изотоническая регрессия | 1.2% |

Лучший результат показала изотоническая регрессия.

## Relevance to Prediction-Market Pricing (EN)

Practical Python guide for calibrating ML models' probability outputs. The ECE reduction from 7.0% to 1.2% via isotonic regression demonstrates that post-hoc calibration should always be applied before using model outputs as Polymarket trading signals.
