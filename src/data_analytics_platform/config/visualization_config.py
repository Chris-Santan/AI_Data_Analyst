# src/config/visualization_config.py
from typing import Dict, Any, List

# Fix the import path
from data_analytics_platform.config.base_config import BaseConfig
# or use relative import:
# from .base_config import BaseConfig


class VisualizationConfig(BaseConfig):
    """
    Configuration for visualization module.
    Contains settings for chart generation, styling, and output formats.
    """

    # Chart types
    BAR = "bar"
    LINE = "line"
    SCATTER = "scatter"
    PIE = "pie"
    BOX = "box"
    HISTOGRAM = "histogram"
    HEATMAP = "heatmap"

    # Themes
    THEME_DEFAULT = "default"
    THEME_DARK = "dark"
    THEME_LIGHT = "light"
    THEME_MINIMAL = "minimal"
    THEME_COLORBLIND = "colorblind"

    def __init__(self, env_prefix: str = "VIZ"):
        """
        Initialize visualization configuration.

        Args:
            env_prefix (str): Prefix for environment variables
        """
        super().__init__("visualization", env_prefix)

        # Default configuration
        self._default_config = {
            'default_chart_type': self.BAR,
            'theme': self.THEME_DEFAULT,
            'use_interactive': True,
            'dpi': 100,
            'figure_size': [10, 6],  # [width, height] in inches
            'font_size': 12,
            'title_font_size': 16,
            'legend_font_size': 10,
            'show_grid': True,
            'export_formats': ['png', 'svg', 'pdf'],
            'export_dir': 'exports/charts',
            'dashboard': {
                'layout': 'grid',  # Options: grid, free
                'rows': 2,
                'cols': 2,
                'title': 'Analytics Dashboard',
                'auto_refresh': False,
                'refresh_interval': 300  # seconds
            },
            'color_palettes': {
                'default': [
                    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728',
                    '#9467bd', '#8c564b', '#e377c2', '#7f7f7f',
                    '#bcbd22', '#17becf'
                ],
                'categorical': [
                    '#4e79a7', '#f28e2c', '#e15759', '#76b7b2',
                    '#59a14f', '#edc949', '#af7aa1', '#ff9da7',
                    '#9c755f', '#bab0ab'
                ],
                'sequential': [
                    '#f7fbff', '#deebf7', '#c6dbef', '#9ecae1',
                    '#6baed6', '#4292c6', '#2171b5', '#08519c',
                    '#08306b'
                ],
                'diverging': [
                    '#8c510a', '#bf812d', '#dfc27d', '#f6e8c3',
                    '#f5f5f5', '#c7eae5', '#80cdc1', '#35978f',
                    '#01665e'
                ]
            },
            'chart_defaults': {
                'bar': {
                    'orientation': 'vertical',
                    'show_values': False,
                    'sort': False
                },
                'line': {
                    'markers': True,
                    'line_width': 2,
                    'show_area': False
                },
                'scatter': {
                    'marker_size': 50,
                    'show_regression': False,
                    'alpha': 0.7
                },
                'pie': {
                    'donut': False,
                    'start_angle': 90,
                    'explode': []
                },
                'box': {
                    'notch': False,
                    'show_fliers': True,
                    'show_mean': True
                },
                'histogram': {
                    'bins': 10,
                    'density': False,
                    'cumulative': False
                },
                'heatmap': {
                    'show_values': True,
                    'center': None,
                    'robust': True
                }
            }
        }

    def get_chart_defaults(self, chart_type: str) -> Dict[str, Any]:
        """
        Get default settings for a specific chart type.

        Args:
            chart_type (str): Type of chart (bar, line, etc.)

        Returns:
            Dict[str, Any]: Chart default settings

        Raises:
            ValueError: If chart type is not found
        """
        chart_defaults = self.get('chart_defaults', {})
        if chart_type not in chart_defaults:
            raise ValueError(f"Chart type not found: {chart_type}")

        return chart_defaults[chart_type]

    def get_color_palette(self, palette_name: str = 'default') -> List[str]:
        """
        Get a color palette by name.

        Args:
            palette_name (str): Name of the palette

        Returns:
            List[str]: List of color codes

        Raises:
            ValueError: If palette is not found
        """
        palettes = self.get('color_palettes', {})
        if palette_name not in palettes:
            raise ValueError(f"Color palette not found: {palette_name}")

        return palettes[palette_name]

    def get_export_formats(self) -> List[str]:
        """
        Get supported export formats.

        Returns:
            List[str]: List of export formats
        """
        return self.get('export_formats', ['png'])

    def get_dashboard_config(self) -> Dict[str, Any]:
        """
        Get dashboard configuration.

        Returns:
            Dict[str, Any]: Dashboard configuration
        """
        return self.get('dashboard', {})

    def is_interactive(self) -> bool:
        """
        Check if interactive charts are enabled.

        Returns:
            bool: True if interactive charts are enabled
        """
        return self.get('use_interactive', True)