"""
Веб-дашборд для визуализации данных энергопотребления в реальном времени.
Использует Streamlit для быстрого создания интерактивного интерфейса.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import psycopg2
from datetime import datetime, timedelta
import asyncio
import threading
import time
import json
from typing import Dict, List, Optional
import logging

# Настройка страницы Streamlit
st.set_page_config(
    page_title="Energy Consumption Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EnergyDashboard:
    """Класс дашборда энергопотребления."""
    
    def __init__(self):
        self.db_connection = None
        self.last_update = None
        self.data_cache = {}
        self.cache_ttl = 30  # секунды
        
    def connect_database(self):
        """Подключение к базе данных с приоритетом переменных окружения."""
        import os
        try:
            # Используем переменные окружения как основной источник, secrets.toml как fallback
            host = os.environ.get("DB_HOST") or st.secrets.get("DB_HOST", "localhost")
            port = int(os.environ.get("DB_PORT") or st.secrets.get("DB_PORT", 5432))
            database = os.environ.get("DB_NAME") or st.secrets.get("DB_NAME", "energy")
            user = os.environ.get("DB_USER") or st.secrets.get("DB_USER", "postgres")
            password = os.environ.get("DB_PASSWORD") or st.secrets.get("DB_PASSWORD", "password")
            
            logger.info(f"Attempting to connect to database with: host={host}, port={port}, db={database}, user={user}")
            
            self.db_connection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            logger.info("Connected to database successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            logger.error(f"Environment variables: DB_HOST={os.environ.get('DB_HOST')}, DB_PORT={os.environ.get('DB_PORT')}, DB_NAME={os.environ.get('DB_NAME')}")
            logger.error(f"Secrets available: {list(st.secrets.keys()) if hasattr(st.secrets, 'keys') else 'No secrets'}")
            return False
    
    def get_recent_readings(self, limit: int = 1000) -> pd.DataFrame:
        """Получение последних показаний из базы данных."""
        query = """
            SELECT meter_id, timestamp, power, validated
            FROM meter_readings
            WHERE timestamp > NOW() - INTERVAL '1 hour'
            ORDER BY timestamp DESC
            LIMIT %s
        """
        
        try:
            with self.db_connection.cursor() as cursor:
                cursor.execute(query, (limit,))
                rows = cursor.fetchall()
                
            df = pd.DataFrame(rows, columns=['meter_id', 'timestamp', 'power', 'validated'])
            return df
            
        except Exception as e:
            logger.error(f"Failed to get recent readings: {e}")
            return pd.DataFrame()
    
    def get_aggregated_data(self, window_minutes: int = 5) -> pd.DataFrame:
        """Получение агрегированных данных за указанный период."""
        query = """
            SELECT 
                meter_id,
                window_start,
                window_end,
                sum_power,
                avg_power,
                min_power,
                max_power,
                count_readings
            FROM aggregated_readings
            WHERE window_start > NOW() - INTERVAL '24 hours'
            ORDER BY window_start DESC
        """
        
        try:
            with self.db_connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                
            df = pd.DataFrame(rows, columns=[
                'meter_id', 'window_start', 'window_end',
                'sum_power', 'avg_power', 'min_power', 'max_power', 'count_readings'
            ])
            return df
            
        except Exception as e:
            logger.error(f"Failed to get aggregated data: {e}")
            return pd.DataFrame()
    
    def get_meter_stats(self) -> pd.DataFrame:
        """Получение статистики по каждому счётчику."""
        query = """
            SELECT 
                meter_id,
                COUNT(*) as total_readings,
                AVG(power) as avg_power,
                MIN(power) as min_power,
                MAX(power) as max_power,
                SUM(power) as total_power,
                MIN(timestamp) as first_reading,
                MAX(timestamp) as last_reading
            FROM meter_readings
            WHERE timestamp > NOW() - INTERVAL '24 hours'
            GROUP BY meter_id
            ORDER BY meter_id
        """
        
        try:
            with self.db_connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                
            df = pd.DataFrame(rows, columns=[
                'meter_id', 'total_readings', 'avg_power', 'min_power',
                'max_power', 'total_power', 'first_reading', 'last_reading'
            ])
            return df
            
        except Exception as e:
            logger.error(f"Failed to get meter stats: {e}")
            return pd.DataFrame()
    
    def get_system_stats(self) -> Dict:
        """Получение общей статистики системы."""
        query = """
            SELECT 
                COUNT(DISTINCT meter_id) as active_meters,
                COUNT(*) as total_readings_last_hour,
                AVG(power) as avg_power_last_hour,
                SUM(power) as total_power_last_hour,
                MAX(timestamp) as latest_reading
            FROM meter_readings
            WHERE timestamp > NOW() - INTERVAL '1 hour'
        """
        
        try:
            with self.db_connection.cursor() as cursor:
                cursor.execute(query)
                row = cursor.fetchone()
                
            return {
                'active_meters': row[0] if row else 0,
                'total_readings_last_hour': row[1] if row else 0,
                'avg_power_last_hour': float(row[2]) if row and row[2] else 0.0,
                'total_power_last_hour': float(row[3]) if row and row[3] else 0.0,
                'latest_reading': row[4] if row else None
            }
            
        except Exception as e:
            logger.error(f"Failed to get system stats: {e}")
            return {}
    
    def create_power_timeseries_chart(self, df: pd.DataFrame) -> go.Figure:
        """Создание графика временных рядов мощности."""
        if df.empty:
            return go.Figure()
        
        fig = go.Figure()
        
        # Группировка по счётчикам
        for meter_id in df['meter_id'].unique()[:10]:  # Ограничиваем 10 счётчиками для читаемости
            meter_df = df[df['meter_id'] == meter_id]
            fig.add_trace(go.Scatter(
                x=meter_df['timestamp'],
                y=meter_df['power'],
                mode='lines+markers',
                name=meter_id,
                line=dict(width=2),
                marker=dict(size=4)
            ))
        
        fig.update_layout(
            title='Мощность по счётчикам (последний час)',
            xaxis_title='Время',
            yaxis_title='Мощность (кВт·ч)',
            hovermode='x unified',
            height=500,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            )
        )
        
        return fig
    
    def create_aggregated_chart(self, df: pd.DataFrame) -> go.Figure:
        """Создание графика агрегированных данных."""
        if df.empty:
            return go.Figure()
        
        # Фильтрация данных для одного счётчика (например, первого)
        sample_meter = df['meter_id'].iloc[0] if len(df) > 0 else None
        if sample_meter:
            meter_df = df[df['meter_id'] == sample_meter].head(20)  # Последние 20 окон
        else:
            meter_df = df.head(20)
        
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Суммарная мощность', 'Средняя мощность', 'Минимальная мощность', 'Максимальная мощность'),
            vertical_spacing=0.15,
            horizontal_spacing=0.1
        )
        
        # Суммарная мощность
        fig.add_trace(
            go.Bar(
                x=meter_df['window_start'],
                y=meter_df['sum_power'],
                name='Сумма',
                marker_color='royalblue'
            ),
            row=1, col=1
        )
        
        # Средняя мощность
        fig.add_trace(
            go.Scatter(
                x=meter_df['window_start'],
                y=meter_df['avg_power'],
                mode='lines+markers',
                name='Средняя',
                line=dict(color='firebrick', width=3)
            ),
            row=1, col=2
        )
        
        # Минимальная мощность
        fig.add_trace(
            go.Scatter(
                x=meter_df['window_start'],
                y=meter_df['min_power'],
                mode='lines+markers',
                name='Минимум',
                line=dict(color='green', width=2)
            ),
            row=2, col=1
        )
        
        # Максимальная мощность
        fig.add_trace(
            go.Scatter(
                x=meter_df['window_start'],
                y=meter_df['max_power'],
                mode='lines+markers',
                name='Максимум',
                line=dict(color='orange', width=2)
            ),
            row=2, col=2
        )
        
        fig.update_layout(
            title=f'Агрегированные данные (счётчик: {sample_meter})',
            height=600,
            showlegend=False
        )
        
        # Обновление осей
        fig.update_xaxes(title_text="Время начала окна", row=2, col=1)
        fig.update_xaxes(title_text="Время начала окна", row=2, col=2)
        fig.update_yaxes(title_text="Мощность (кВт·ч)", row=1, col=1)
        fig.update_yaxes(title_text="Мощность (кВт·ч)", row=1, col=2)
        fig.update_yaxes(title_text="Мощность (кВт·ч)", row=2, col=1)
        fig.update_yaxes(title_text="Мощность (кВт·ч)", row=2, col=2)
        
        return fig
    
    def create_distribution_chart(self, df: pd.DataFrame) -> go.Figure:
        """Создание графика распределения мощности."""
        if df.empty:
            return go.Figure()
        
        fig = px.histogram(
            df, 
            x='power',
            nbins=50,
            title='Распределение мощности',
            labels={'power': 'Мощность (кВт·ч)', 'count': 'Количество показаний'},
            color_discrete_sequence=['indianred']
        )
        
        fig.update_layout(
            height=400,
            bargap=0.1
        )
        
        return fig
    
    def create_meter_summary_table(self, df: pd.DataFrame):
        """Создание таблицы сводки по счётчикам."""
        if df.empty:
            st.write("Нет данных для отображения")
            return
        
        # Форматирование данных для таблицы
        display_df = df.copy()
        display_df['avg_power'] = display_df['avg_power'].round(3)
        display_df['total_power'] = display_df['total_power'].round(2)
        display_df['last_reading'] = display_df['last_reading'].dt.strftime('%H:%M:%S')
        
        # Отображение таблицы
        st.dataframe(
            display_df[['meter_id', 'total_readings', 'avg_power', 'min_power', 'max_power', 'total_power', 'last_reading']],
            use_container_width=True,
            height=400
        )

def main():
    """Основная функция дашборда."""
    st.title("⚡ Дашборд энергопотребления")
    st.markdown("Мониторинг и анализ данных со счётчиков электроэнергии в реальном времени")
    
    # Инициализация дашборда
    dashboard = EnergyDashboard()
    
    # Подключение к базе данных
    if not dashboard.connect_database():
        st.error("Не удалось подключиться к базе данных. Проверьте настройки подключения.")
        return
    
    # Сайдбар с настройками
    with st.sidebar:
        st.header("Настройки")
        
        # Период обновления
        refresh_interval = st.slider(
            "Интервал обновления (секунды)",
            min_value=5,
            max_value=60,
            value=30,
            step=5
        )
        
        # Количество счётчиков для отображения
        max_meters = st.slider(
            "Максимальное количество счётчиков на графике",
            min_value=5,
            max_value=50,
            value=10,
            step=5
        )
        
        # Период данных
        time_range = st.selectbox(
            "Период данных",
            ["1 час", "6 часов", "24 часа", "7 дней"],
            index=0
        )
        
        # Кнопка обновления
        if st.button("Обновить данные", type="primary"):
            st.rerun()
        
        st.divider()
        
        # Информация о системе
        st.header("Информация о системе")
        system_stats = dashboard.get_system_stats()
        
        st.metric("Активные счётчики", system_stats.get('active_meters', 0))
        st.metric("Показаний за час", f"{system_stats.get('total_readings_last_hour', 0):,}")
        st.metric("Средняя мощность", f"{system_stats.get('avg_power_last_hour', 0):.2f} кВт·ч")
        st.metric("Общая мощность", f"{system_stats.get('total_power_last_hour', 0):.2f} кВт·ч")
        
        if system_stats.get('latest_reading'):
            st.caption(f"Последнее обновление: {system_stats['latest_reading'].strftime('%H:%M:%S')}")
    
    # Основное содержимое
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Обзор", 
        "📈 Графики", 
        "🔢 Агрегированные данные", 
        "📋 Статистика"
    ])
    
    with tab1:
        st.header("Обзор системы")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.subheader("Текущая активность")
            recent_df = dashboard.get_recent_readings(limit=100)
            if not recent_df.empty:
                latest_power = recent_df['power'].iloc[0] if len(recent_df) > 0 else 0
                st.metric("Последняя мощность", f"{latest_power:.2f} кВт·ч")
                
                latest_meter = recent_df['meter_id'].iloc[0] if len(recent_df) > 0 else "N/A"
                st.metric("Последний счётчик", latest_meter)
            else:
                st.info("Нет данных о текущей активности")
        
        with col2:
            st.subheader("Качество данных")
            if not recent_df.empty:
                valid_percentage = (recent_df['validated'].sum() / len(recent_df)) * 100
                st.metric("Валидные данные", f"{valid_percentage:.1f}%")
                
                unique_meters = recent_df['meter_id'].nunique()
                st.metric("Уникальные счётчики", unique_meters)
            else:
                st.info("Нет данных о качестве")
        
        with col3:
            st.subheader("Производительность")
            system_stats = dashboard.get_system_stats()
            readings_per_minute = system_stats.get('total_readings_last_hour', 0) / 60
            st.metric("Показаний в минуту", f"{readings_per_minute:.1f}")
            
            if system_stats.get('latest_reading'):
                time_diff = datetime.now() - system_stats['latest_reading']
                latency = time_diff.total_seconds()
                st.metric("Задержка данных", f"{latency:.1f} сек")
        
        st.divider()
        
        # График временных рядов
        st.subheader("Мощность в реальном времени")
        recent_df = dashboard.get_recent_readings(limit=1000)
        if not recent_df.empty:
            fig = dashboard.create_power_timeseries_chart(recent_df)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Нет данных для построения графика")
    
    with tab2:
        st.header("Детальные графики")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Распределение мощности")
            recent_df = dashboard.get_recent_readings(limit=5000)
            if not recent_df.empty:
                fig = dashboard.create_distribution_chart(recent_df)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Нет данных для построения распределения")
        
        with col2:
            st.subheader("Тепловая карта активности")
            if not recent_df.empty:
                # Создание тепловой карты по часам и счётчикам
                recent_df['hour'] = recent_df['timestamp'].dt.hour
                pivot_df = recent_df.pivot_table(
                    index='meter_id',
                    columns='hour',
                    values='power',
                    aggfunc='mean'
                ).fillna(0)
                
                if not pivot_df.empty:
                    fig = px.imshow(
                        pivot_df,
                        title='Средняя мощность по часам',
                        labels=dict(x="Час", y="Счётчик", color="Мощность"),
                        aspect="auto"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Недостаточно данных для тепловой карты")
            else:
                st.info("Нет данных для тепловой карты")
    
    with tab3:
        st.header("Агрегированные данные")
        
        # Выбор периода агрегации
        agg_period = st.selectbox(
            "Период агрегации",
            ["5 минут", "15 минут", "30 минут", "1 час"],
            index=0
        )
        
        # Получение агрегированных данных
        agg_df = dashboard.get_aggregated_data()
        
        if not agg_df.empty:
            # График агрегированных данных
            fig = dashboard.create_aggregated_chart(agg_df)
            st.plotly_chart(fig, use_container_width=True)
            
            # Таблица агрегированных данных
            st.subheader("Данные агрегации")
            st.dataframe(
                agg_df.head(20),
                use_container_width=True,
                height=400
            )
        else:
            st.info("Нет агрегированных данных для отображения")
    
    with tab4:
        st.header("Статистика по счётчикам")
        
        # Получение статистики
        stats_df = dashboard.get_meter_stats()
        
        if not stats_df.empty:
            # Таблица статистики
            dashboard.create_meter_summary_table(stats_df)
            
            # Дополнительная визуализация
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Топ-10 счётчиков по мощности")
                top_meters = stats_df.nlargest(10, 'total_power')
                fig = px.bar(
                    top_meters,
                    x='meter_id',
                    y='total_power',
                    title='Общая мощность по счётчикам',
                    labels={'meter_id': 'Счётчик', 'total_power': 'Общая мощность (кВт·ч)'},
                    color='total_power',
                    color_continuous_scale='Viridis'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("Активность счётчиков")
                fig = px.scatter(
                    stats_df,
                    x='total_readings',
                    y='avg_power',
                    size='max_power',
                    color='meter_id',
                    title='Активность vs Средняя мощность',
                    labels={'total_readings': 'Количество показаний', 'avg_power': 'Средняя мощность'},
                    hover_data=['meter_id', 'min_power', 'max_power']
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Нет статистических данных для отображения")
    
    # Автоматическое обновление
    if refresh_interval > 0:
        time.sleep(refresh_interval)
        st.rerun()

if __name__ == "__main__":
    main()