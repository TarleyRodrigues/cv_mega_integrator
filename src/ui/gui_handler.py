# src/ui/gui_handler.py
import tkinter as tk
from tkinter import filedialog, messagebox, ttk


class AppGUI:
    def __init__(self, mapping, start_callback):
        self.mapping = mapping
        self.start_callback = start_callback
        self.root = tk.Tk()
        self.root.title("Integrador CV CRM <-> MEGA ERP")
        self.root.geometry("600x800")

        self.manual_inputs = {}
        self.mega_file_path = tk.StringVar()

        self._build_ui()

    def _build_ui(self):
        # Container Principal com Scrollbar
        main_canvas = tk.Canvas(self.root)
        scrollbar = ttk.Scrollbar(
            self.root, orient="vertical", command=main_canvas.yview)
        self.scrollable_frame = ttk.Frame(main_canvas)

        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: main_canvas.configure(
                scrollregion=main_canvas.bbox("all"))
        )

        main_canvas.create_window(
            (0, 0), window=self.scrollable_frame, anchor="nw")
        main_canvas.configure(yscrollcommand=scrollbar.set)

        # Seção de Arquivos
        file_frame = ttk.LabelFrame(
            self.scrollable_frame, text=" Arquivos de Origem ", padding=10)
        file_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(file_frame, text="Arquivo MEGA (.xls/.xlsx):").pack(anchor="w")
        ttk.Entry(file_frame, textvariable=self.mega_file_path,
                  width=50).pack(side="left", padx=5)
        ttk.Button(file_frame, text="Procurar",
                   command=self._browse_file).pack(side="left")

        # Seção de Dados Manuais (Gerada Dinamicamente)
        manual_frame = ttk.LabelFrame(
            self.scrollable_frame, text=" Dados do Empreendimento ", padding=10)
        manual_frame.pack(fill="x", padx=10, pady=5)

        manual_cols = {k: v for k, v in self.mapping['columns'].items() if v.get(
            'origin') == 'manual'}

        for col_name, config in manual_cols.items():
            lbl_frame = ttk.Frame(manual_frame)
            lbl_frame.pack(fill="x", pady=2)

            ttk.Label(lbl_frame, text=col_name, width=30).pack(side="left")

            var = tk.StringVar(value=config.get("default", ""))
            entry = ttk.Entry(lbl_frame, textvariable=var)
            entry.pack(side="right", fill="x", expand=True)
            self.manual_inputs[col_name] = var

        # Botão Processar
        btn_process = ttk.Button(
            self.scrollable_frame,
            text="GERAR PLANILHA CV CRM",
            command=self._on_submit,
            style="Accent.TButton"
        )
        btn_process.pack(pady=20)

        main_canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

    def _browse_file(self):
        filename = filedialog.askopenfilename(
            filetypes=[("Excel files", "*.xls *.xlsx")])
        if filename:
            self.mega_file_path.set(filename)

    def _on_submit(self):
        if not self.mega_file_path.get():
            messagebox.showerror("Erro", "Selecione o arquivo do MEGA ERP")
            return

        # Converte StringVar para dict simples
        manual_data = {k: v.get() for k, v in self.manual_inputs.items()}

        # Pergunta onde salvar
        save_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            initialfile="importacao_cv.csv"
        )

        if save_path:
            self.start_callback(self.mega_file_path.get(),
                                manual_data, save_path)
            messagebox.showinfo("Sucesso", f"Arquivo gerado em:\n{save_path}")

    def run(self):
        self.root.mainloop()
