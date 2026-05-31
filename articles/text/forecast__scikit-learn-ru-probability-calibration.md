---
title: "Калибровка вероятности — scikit-learn документация (RU)"
url: https://scikit-learn.ru/stable/modules/calibration.html
source: scikit-learn-ru
date: "2024-01-01"
type: documentation
theme: forecast
lang: ru
---

# Калибровка вероятности — scikit-learn 1.5.2 документация

**Источник:** scikit-learn.ru (русская документация scikit-learn)
**URL:** https://scikit-learn.ru/stable/modules/calibration.html

## Что такое калибровка вероятности?

Калибровка вероятности — степень, с которой прогнозируемые в модели классификации вероятности соответствуют истинной частотности целевых классов в наборе данных.

Хорошо откалиброванная модель: если из множества прогнозов, для которых предсказана 70%-ная вероятность положительного класса, модель корректна в 70% случаев.

## Оценка качества калибровки

**Calibration plot (график калибровки):**
- По оси X: предсказанная вероятность положительного класса
- По оси Y: фактическая частотность
- Идеальная модель: зависимость Y = X (диагональ)

**Характерные паттерны:**
- Naive Bayes: недооценивает при малых вероятностях, переоценивает при высоких (S-образная кривая)
- SVM: переуверенная (резкие переходы к 0 и 1)
- Random Forest: слегка недооценивает высокие вероятности

**Численные метрики:**
- Expected Calibration Error (ECE)
- Maximum Calibration Error (MCE)
- Brier Score

## Методы калибровки (Platt и Isotonic)

**Platt Scaling (Сигмоидальная калибровка):**
```python
from sklearn.calibration import CalibratedClassifierCV
calibrated = CalibratedClassifierCV(base_clf, method='sigmoid', cv=5)
```
Подходит для малых наборов данных. Предполагает сигмоидальную форму калибровочной кривой.

**Isotonic Regression (Изотоническая регрессия):**
```python
calibrated = CalibratedClassifierCV(base_clf, method='isotonic', cv=5)
```
Более мощный, нелинейный метод. Требует больше данных. Обычно лучше для больших наборов.

## Calibration Curve

```python
from sklearn.calibration import calibration_curve
fraction_of_positives, mean_predicted_value = calibration_curve(y_true, y_prob, n_bins=10)
```

## Relevance to Prediction-Market Pricing (EN)

Russian-language reference for applying scikit-learn's calibration tools. The CalibratedClassifierCV with isotonic method is the recommended approach for post-hoc calibration of any ML model used to generate probability estimates for Polymarket trading signals. ECE metric enables tracking calibration quality over time.
