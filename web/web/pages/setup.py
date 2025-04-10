from __future__ import annotations

import json
import typing as t
from datetime import date
import rio
#from app.utils.config_loader import load_config
import app.utils.config_loader

@rio.page(
    name='Setup',
    url_segment='setup',
)
class Setup(rio.Component):
    config: t.Dict[str, t.Any]
    server_url: str = ""
    start_date: str = ""
    export_enabled: bool = False

    def __init__(
            self,
            config: t.Dict[str, t.Any],
            start_date: t.Optional[str] = None,
    ) -> None:
        """

            start_date: Начальная дата в формате YYYY-MM-DD
        """
        super().__init__()

        if not config:
            raise ValueError("Не передана конфигурация БД")

        self.config = config
        self.server_url = config.get("api_url", "")
        self.export_enabled = config.get("export_enabled", False)
        self.start_date = start_date or str(date.today())

    def handle_date_change(self, text: str) -> None:
        """Обработчик изменения даты"""
        self.start_date = text
        self.config["start_date"] = text
        self._update_config_json()

    def handle_url_change(self, text: str) -> None:
        """Обработчик изменения URL"""
        self.server_url = text
        self.config["api_url"] = text
        self._update_config_json()

    def handle_export_toggle(self, value: bool) -> None:
        """Обработчик переключения экспорта"""
        self.export_enabled = value
        self.config["export_enabled"] = value
        self._update_config_json()

    def _update_config_json(self) -> None:
        """Обновляет JSON конфигурации"""
        try:
            self.configuration_json = json.dumps(self.config, indent=2)
            print(f"Конфигурация обновлена:\n{self.configuration_json}")
        except Exception as e:
            print(f"Ошибка сериализации конфига: {e}")

    def handle_save(self) -> None:
        """Обработчик сохранения настроек"""
        self._update_config_json()
        # Здесь можно добавить логику сохранения в файл/БД
        print("Настройки сохранены!")

    def build(self) -> rio.Component:
        return rio.Column(
            rio.Card(
                rio.Text("Меню настройки выгрузки", style="heading2"),
                margin=1,
                corner_radius=0.4,
            ),
            rio.Column(
                rio.Row(
                    rio.Text("URL сервера:", width=15),
                    rio.TextInput(
                        text=self.server_url,
                        on_change=self.handle_url_change,
                        width=25,
                    ),
                    align_y=0.5,
                    spacing=1,
                ),
                rio.Row(
                    rio.Text("Дата начала:", width=15),
                    rio.TextInput(
                        text=self.start_date,
                        on_change=self.handle_date_change,
                        style="pill",
                        width=25,
                    ),
                    align_y=0.5,
                    spacing=1,
                ),
                rio.Row(
                    rio.Text("Экспорт:", width=15),
                    rio.Checkbox(
                        is_checked=self.export_enabled,
                        on_change=self.handle_export_toggle,
                    ),
                    align_y=0.5,
                    spacing=1,
                ),
                rio.Row(
                    rio.Spacer(),
                    rio.Button(
                        "Сохранить настройки",
                        on_press=self.handle_save,
                        shape="rectangle",
                        style="major",
                    ),
                    width="grow",
                    margin_top=1,
                ),
                spacing=2,
                margin=2,
            ),
            spacing=1,
            width=50,
        )