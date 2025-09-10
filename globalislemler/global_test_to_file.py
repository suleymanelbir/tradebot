import ast
import os
import tokenize
import re
import inspect
import asyncio
import time
from pathlib import Path
from io import StringIO
from typing import List, Dict, Tuple, Set, Any, Optional, Union
from datetime import datetime
import math
from collections import defaultdict
import importlib

RISKLI_IFADELER = {
    "eval", "exec", "os.system", "subprocess", "open", "requests.get",
    "pickle.loads", "marshal.loads", "yaml.load", "execfile"
}

GUVENLI_ALTERNATIFLER = {
    "eval": "ast.literal_eval",
    "exec": "Yorumlanabilir kod yerine native Python kullanın",
    "os.system": "subprocess.run with shell=False",
    "open": "Context manager (with open()) kullanın",
    "pickle.loads": "JSON veya safer serialization formatları"
}

PEP8_ISIM_KURALLARI = {
    "degisken": r"^[a-z_][a-z0-9_]*$",
    "sabit": r"^[A-Z_][A-Z0-9_]*$", 
    "sinif": r"^[A-Z][a-zA-Z0-9]*$",
    "metod": r"^[a-z_][a-z0-9_]*$",
    "fonksiyon": r"^[a-z_][a-z0-9_]*$"
}

class FonksiyonZiyaretci(ast.NodeVisitor):
    def __init__(self):
        self.fonksiyonlar = []
        self.stack = []
        self.siniflar = []
        self.ic_ice_fonksiyonlar = []
        self.lambda_sayaci = 0
        self.walrus_operator_sayaci = 0
        self.async_fonksiyonlar = []
        self.import_edilen_kutuphaneler = set()
        self.kullanilan_kutuphaneler = set()

    def visit_FunctionDef(self, node):
        parent = self.stack[-1] if self.stack else None
        self.fonksiyonlar.append((node, parent))
        
        if parent and isinstance(parent, ast.FunctionDef):
            self.ic_ice_fonksiyonlar.append((node, parent))
        
        self.stack.append(node)
        self.generic_visit(node)
        self.stack.pop()

    def visit_AsyncFunctionDef(self, node):
        self.async_fonksiyonlar.append(node)
        self.visit_FunctionDef(node)

    def visit_ClassDef(self, node):
        self.siniflar.append(node)
        self.stack.append(node)
        self.generic_visit(node)
        self.stack.pop()

    def visit_Lambda(self, node):
        self.lambda_sayaci += 1
        self.generic_visit(node)

    def visit_NamedExpr(self, node):
        self.walrus_operator_sayaci += 1
        self.generic_visit(node)

    def visit_Import(self, node):
        for alias in node.names:
            self.import_edilen_kutuphaneler.add(alias.name.split('.')[0])
        self.generic_visit(node)

def kontrol_parantez_dengesi_detayli(kaynak_kod: str) -> Tuple[bool, List[str]]:
    """Detaylı parantez, köşeli parantez ve süslü parantez dengesi kontrolü"""
    hatalar = []
    stack = []
    
    for satir_no, satir in enumerate(kaynak_kod.splitlines(), 1):
        for char_no, char in enumerate(satir, 1):
            if char in '({[':
                stack.append((char, satir_no, char_no))
            elif char in ')}]':
                if not stack:
                    hatalar.append(f"Satır {satir_no}: Açılmamış '{char}' kapatılmaya çalışılıyor")
                else:
                    son_acik, acik_satir, acik_char = stack.pop()
                    if (char == ')' and son_acik != '(') or \
                       (char == '}' and son_acik != '{') or \
                       (char == ']' and son_acik != '['):
                        hatalar.append(f"Satır {satir_no}: '{son_acik}' ile açılan parantez '{char}' ile kapatılıyor")
    
    # Açık kalan parantezler
    for acik_parantez, satir_no, char_no in stack:
        hatalar.append(f"Satır {satir_no}: Açık kalan '{acik_parantez}'")
    
    return len(hatalar) == 0, hatalar

def kontrol_dongu_kompleksitesi(fonk_node: ast.FunctionDef) -> Tuple[int, List[str]]:
    """Döngü yapılarının kompleksitesini analiz eder"""
    dongu_analiz = []
    puan = 5  # Başlangıç puanı
    
    for node in ast.walk(fonk_node):
        if isinstance(node, ast.For):
            # İç içe döngü kontrolü
            ic_ice_derinlik = 0
            current = node
            while current:
                if hasattr(current, 'parent'):
                    current = current.parent
                    if isinstance(current, (ast.For, ast.While)):
                        ic_ice_derinlik += 1
                else:
                    break
            
            if ic_ice_derinlik > 2:
                dongu_analiz.append(f"⚠️ İç içe döngü derinliği: {ic_ice_derinlik} (performans riski)")
                puan -= 2
            elif ic_ice_derinlik > 0:
                dongu_analiz.append(f"ℹ️ İç içe döngü derinliği: {ic_ice_derinlik}")
            
            # Break/Continue/Else kontrolü
            break_count = sum(1 for n in ast.walk(node) if isinstance(n, ast.Break))
            continue_count = sum(1 for n in ast.walk(node) if isinstance(n, ast.Continue))
            
            if break_count > 3:
                dongu_analiz.append("⚠️ Çok fazla break kullanımı (kodu zorlaştırır)")
                puan -= 1
            if continue_count > 3:
                dongu_analiz.append("⚠️ Çok fazla continue kullanımı")
                puan -= 1
        
        elif isinstance(node, ast.While):
            # Sonsuz döngü riski
            condition = ast.unparse(node.test)
            if condition in ['True', '1', 'True']:
                dongu_analiz.append("⚠️ Potansiyel sonsuz döngü (while True)")
                puan -= 2
    
    return max(0, puan), dongu_analiz

def kontrol_try_except_detayli(fonk_node: ast.FunctionDef) -> Tuple[int, List[str]]:
    """Detaylı try-except analizi"""
    try_nodes = [n for n in ast.walk(fonk_node) if isinstance(n, ast.Try)]
    
    if not try_nodes:
        return 0, ["❌ Try/Except bloğu yok"]
    
    analizler = []
    puan = 0
    
    for try_node in try_nodes:
        # Exception türleri
        except_analiz = []
        for handler in try_node.handlers:
            if handler.type is None:
                except_analiz.append("Exception (genel - ⚠️ riskli)")
                puan -= 2
            else:
                type_name = ast.unparse(handler.type)
                if type_name == "Exception":
                    except_analiz.append(f"{type_name} (⚠️ çok genel)")
                    puan -= 1
                else:
                    except_analiz.append(f"{type_name} (✅ spesifik)")
                    puan += 2
        
        # Finally/Else blokları
        extras = []
        if try_node.finalbody:
            extras.append("finally")
            puan += 1
        if try_node.orelse:
            extras.append("else")
            puan += 1
        
        analiz = f"Yakalanan hatalar: {', '.join(except_analiz)}"
        if extras:
            analiz += f", Ek bloklar: {', '.join(extras)}"
        
        analizler.append(analiz)
    
    return puan, analizler

def kontrol_memory_impact(fonk_node: ast.FunctionDef) -> Tuple[int, List[str]]:
    """Memory kullanımı ve potansiyel leak analizi"""
    uyarilar = []
    puan = 5
    
    # Büyük veri yapıları
    for node in ast.walk(fonk_node):
        if isinstance(node, ast.List) or isinstance(node, ast.Dict) or isinstance(node, ast.Set):
            eleman_sayisi = len(node.elts) if hasattr(node, 'elts') else 0
            if eleman_sayisi > 100:
                uyarilar.append(f"⚠️ Büyük veri yapısı ({eleman_sayisi} eleman)")
                puan -= 1
        
        # Global değişken atamaları
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and not any(
                    isinstance(parent, (ast.FunctionDef, ast.ClassDef)) 
                    for parent in ast.walk(fonk_node)
                ):
                    uyarilar.append("⚠️ Global scope'ta değişken ataması")
                    puan -= 2
    
    return puan, uyarilar

def bul_tekrarlanan_kod(fonk_listesi: List[Tuple[ast.FunctionDef, Any]]) -> List[Tuple[str, str]]:
    """Benzer fonksiyon ve kod yapılarını bulur"""
    benzerlikler = []
    
    for i, (fonk1, _) in enumerate(fonk_listesi):
        for j, (fonk2, _) in enumerate(fonk_listesi[i+1:], i+1):
            # Basit benzerlik kontrolü (parametre sayısı ve karmaşıklık)
            param_benzerlik = len(fonk1.args.args) == len(fonk2.args.args)
            
            karmaşıklık1 = sum(1 for n in ast.walk(fonk1) 
                             if isinstance(n, (ast.If, ast.For, ast.While)))
            karmaşıklık2 = sum(1 for n in ast.walk(fonk2) 
                             if isinstance(n, (ast.If, ast.For, ast.While)))
            
            karmaşıklık_benzerlik = abs(karmaşıklık1 - karmaşıklık2) <= 2
            
            if param_benzerlik and karmaşıklık_benzerlik:
                benzerlikler.append((fonk1.name, fonk2.name))
    
    return benzerlikler

def analiz_kod_klonlari(kaynak_kod: str) -> List[str]:
    """Basit kod klonları tespiti"""
    klon_analiz = []
    
    # Satır bazında basit tekrar kontrolü
    satirlar = kaynak_kod.splitlines()
    satir_frekans = defaultdict(int)
    
    for satir in satirlar:
        temiz_satir = satir.strip()
        if temiz_satir and not temiz_satir.startswith('#') and len(temiz_satir) > 10:
            satir_frekans[temiz_satir] += 1
    
    tekrarlanan_satirlar = {satir: count for satir, count in satir_frekans.items() if count > 2}
    if tekrarlanan_satirlar:
        klon_analiz.append(f"⚠️ {len(tekrarlanan_satirlar)} tekrarlanan kod satırı bulundu")
    
    return klon_analiz

def kontrol_docstring_kalitesi(fonk_node: ast.FunctionDef) -> Tuple[int, List[str]]:
    """Docstring kalitesini detaylı analiz eder"""
    docstring = ast.get_docstring(fonk_node)
    puan = 0
    oneriler = []
    
    if not docstring:
        return 0, ["❌ Docstring yok"]
    
    # Temel varlık kontrolü
    puan += 1
    
    # Parametre dokümantasyonu
    param_patterns = [r":param", r":type", r":arg", r"parameter", r"Args:"]
    param_var = any(re.search(p, docstring, re.IGNORECASE) for p in param_patterns)
    
    if param_var:
        puan += 2
    else:
        oneriler.append("📝 Parametre dokümantasyonu eksik (':param' veya 'Args:')")
    
    # Return dokümantasyonu
    return_patterns = [r":return", r":rtype", r"Returns:", r"return"]
    return_var = any(re.search(p, docstring, re.IGNORECASE) for p in return_patterns)
    
    if return_var:
        puan += 2
    else:
        oneriler.append("📝 Return dokümantasyonu eksik (':return' veya 'Returns:')")
    
    # Örnek kullanım kontrolü
    example_patterns = [r"Example:", r"Examples:", r">>>", r"Usage:"]
    example_var = any(re.search(p, docstring, re.IGNORECASE) for p in example_patterns)
    
    if example_var:
        puan += 2
        # Örnek kod çalışırlık kontrolü (basit)
        if ">>>" in docstring:
            oneriler.append("✅ Örnek kod snippet'leri var")
    else:
        oneriler.append("📝 Örnek kullanım eksik ('Example:' veya '>>>')")
    
    # Uzunluk kontrolü
    satir_sayisi = len(docstring.splitlines())
    if satir_sayisi >= 3:
        puan += 1
    else:
        oneriler.append("📝 Docstring çok kısa (en az 3 satır önerilir)")
    
    return min(puan, 10), oneriler

def kontrol_type_hint_kapsami(fonk_node: ast.FunctionDef) -> Tuple[int, List[str]]:
    """Type hint kapsam analizi"""
    puan = 0
    oneriler = []
    
    # Return type hint
    if fonk_node.returns:
        puan += 3
    else:
        oneriler.append("🎯 Return type hint eksik -> def func() -> int:")
    
    # Parametre type hints
    parametreler = fonk_node.args.args
    annotated_params = sum(1 for p in parametreler if p.annotation)
    
    if annotated_params == len(parametreler):
        puan += 5
    elif annotated_params > 0:
        puan += 2
        oneriler.append(f"🎯 Bazı parametrelerde type hint eksik ({annotated_params}/{len(parametreler)})")
    else:
        oneriler.append("🎯 Parametre type hint'leri eksik -> def func(param: int):")
    
    # Optional/Union kontrolü
    for param in parametreler:
        if param.annotation:
            annotation_str = ast.unparse(param.annotation)
            if "Optional" in annotation_str or "Union" in annotation_str:
                puan += 1
                break
    
    return min(puan, 10), oneriler

def kontrol_async_uygunluk(fonk_node: ast.FunctionDef) -> Tuple[int, List[str]]:
    """Async fonksiyon kullanım uygunluğu"""
    puan = 5
    oneriler = []
    
    # I/O operasyonları kontrolü (basit)
    io_patterns = [
        "open", "read", "write", "request", "get", "post", 
        "sleep", "time.sleep", "input", "print"
    ]
    
    fonk_kodu = ast.unparse(fonk_node)
    io_operasyonlari = [pattern for pattern in io_patterns if pattern in fonk_kodu]
    
    if io_operasyonlari and not isinstance(fonk_node, ast.AsyncFunctionDef):
        puan -= 2
        oneriler.append(f"⚡ I/O operasyonları var ama async değil: {io_operasyonlari}")
    
    # Async uyumluluk
    if isinstance(fonk_node, ast.AsyncFunctionDef):
        puan += 3
        oneriler.append("✅ Async fonksiyon - modern Python uyumlu")
    
    return max(0, puan), oneriler

def analiz_zaman_karmasikligi(fonk_node: ast.FunctionDef) -> Tuple[str, List[str]]:
    """Zaman karmaşıklığı tahmini (Big-O)"""
    analizler = []
    
    # Döngü analizi
    for dongu in fonk_node.body:
        if isinstance(dongu, (ast.For, ast.While)):
            # İç içe döngü kontrolü
            ic_ice_derinlik = 0
            current = dongu
            while current:
                if hasattr(current, 'parent'):
                    current = current.parent
                    if isinstance(current, (ast.For, ast.While)):
                        ic_ice_derinlik += 1
                else:
                    break
            
            if ic_ice_derinlik >= 2:
                analizler.append(f"⏰ O(n^{ic_ice_derinlik}) - İç içe döngüler")
            elif ic_ice_derinlik == 1:
                analizler.append("⏰ O(n) - Lineer döngü")
    
    # Recursion kontrolü
    recursion_calls = sum(1 for n in ast.walk(fonk_node) 
                         if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) 
                         and n.func.id == fonk_node.name)
    
    if recursion_calls > 0:
        analizler.append(f"⏰ O(2^n) - Recursive fonksiyon ({recursion_calls} çağrı)")
    
    if not analizler:
        analizler.append("⏰ O(1) - Sabit zaman")
    
    return analizler[0], analizler

def kontrol_kutuphane_kullanimi(ziyaretci: FonksiyonZiyaretci) -> Tuple[int, List[str]]:
    """Kütüphane kullanım analizi"""
    kullanilmayan_kutuphaneler = ziyaretci.import_edilen_kutuphaneler - ziyaretci.kullanilan_kutuphaneler
    puan = 10
    oneriler = []
    
    if kullanilmayan_kutuphaneler:
        puan -= len(kullanilmayan_kutuphaneler) * 2
        oneriler.append(f"📦 Kullanılmayan kütüphaneler: {', '.join(kullanilmayan_kutuphaneler)}")
    
    return max(0, puan), oneriler

def kontrol_code_smells(fonk_node: ast.FunctionDef) -> Tuple[int, List[str]]:
    """Code smell detection"""
    puan = 10
    smells = []
    
    # Magic numbers
    magic_numbers = []
    for node in ast.walk(fonk_node):
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            if abs(node.value) not in [0, 1, 2, 10, 100, 1000]:
                magic_numbers.append(str(node.value))
    
    if magic_numbers:
        puan -= 2
        smells.append(f"👃 Magic numbers: {', '.join(set(magic_numbers))}")
    
    # Long method detection
    satir_sayisi = fonk_node.end_lineno - fonk_node.lineno if fonk_node.end_lineno else 0
    if satir_sayisi > 30:
        puan -= 3
        smells.append(f"📏 Uzun metod ({satir_sayisi} satır)")
    
    # Too many parameters
    if len(fonk_node.args.args) > 5:
        puan -= 2
        smells.append(f"🎯 Çok fazla parametre ({len(fonk_node.args.args)})")
    
    return max(0, puan), smells

def kontrol_pep8_isimlendirme(isim: str, tur: str) -> Tuple[bool, str]:
    """PEP8 isimlendirme kurallarını kontrol eder"""
    if tur not in PEP8_ISIM_KURALLARI:
        return True, ""
    
    pattern = PEP8_ISIM_KURALLARI[tur]
    uygun = re.match(pattern, isim) is not None
    
    if not uygun:
        ornekler = {
            "degisken": "örnek_degisken, sayac, kullanici_adi",
            "sabit": "MAX_SAYI, DATABASE_URL, API_KEY", 
            "sinif": "KullaniciManager, VeriTabaniBaglantisi",
            "metod": "kaydet, verileri_getir, hesapla",
            "fonksiyon": "dosya_oku, veriyi_isle, rapor_olustur"
        }
        return False, f"⚠️ {tur} isimlendirme: '{isim}'. Örnek: {ornekler[tur]}"
    
    return True, "✅ PEP8 uyumlu"

def kontrol_girinti_detayli(kaynak_kod: str) -> str:
    """Detaylı girinti kontrolü"""
    try:
        tokens = list(tokenize.generate_tokens(StringIO(kaynak_kod).readline))
        hatalar = []
        
        for i, (toknum, tokval, start, end, line) in enumerate(tokens):
            if toknum == tokenize.INDENT:
                if "\t" in tokval:
                    hatalar.append(f"Satır {start[0]}: Tab karakteri kullanımı")
                
                if len(tokval) % 4 != 0:
                    hatalar.append(f"Satır {start[0]}: 4'ün katı olmayan girinti ({len(tokval)} boşluk)")
        
        if not hatalar:
            return "✅ Mükemmel girinti ve boşluk kullanımı"
        else:
            return f"⚠️ Girinti sorunları: {', '.join(hatalar[:3])}{'...' if len(hatalar) > 3 else ''}"
            
    except Exception as e:
        return f"⚠️ Girinti analizi başarısız: {str(e)}"

def analiz_yap(dosya_yolu: str) -> str:
    """Süper geliştirilmiş kod analizi"""
    if not os.path.exists(dosya_yolu):
        return f"❌ Hata: Dosya bulunamadı → {dosya_yolu}"

    try:
        with open(dosya_yolu, "r", encoding="utf-8") as f:
            kaynak_kod = f.read()
        agac = ast.parse(kaynak_kod)
    except Exception as e:
        return f"❌ Dosya okuma/parsing hatası: {e}"

    analiz = []
    satirlar = kaynak_kod.splitlines()
    
    # GENEL METRİKLER
    yorum_satir = sum(1 for s in satirlar if s.strip().startswith("#"))
    bos_satir = sum(1 for s in satirlar if not s.strip())
    
    analiz.append("🎯 SÜPER KOD ANALİZ SİSTEMİ")
    analiz.append("=" * 80)
    analiz.append(f"📁 Dosya: {dosya_yolu}")
    analiz.append(f"📅 Analiz: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    analiz.append("")
    
    # PARANTEZ DENGESİ KONTROLÜ
    parantez_dengesi, parantez_hatalari = kontrol_parantez_dengesi_detayli(kaynak_kod)
    analiz.append("🔗 PARANTEZ DENGESİ")
    analiz.append("-" * 40)
    analiz.append(f"  • Durum: {'✅ Dengeli' if parantez_dengesi else '❌ Dengesiz'}")
    if not parantez_dengesi:
        for hata in parantez_hatalari[:3]:
            analiz.append(f"  • {hata}")
        if len(parantez_hatalari) > 3:
            analiz.append(f"  • ... ve {len(parantez_hatalari) - 3} hata daha")
    analiz.append("")

    # GİRİNTİ KONTROLÜ
    girinti_analiz = kontrol_girinti_detayli(kaynak_kod)
    analiz.append("📐 GİRİNTİ ANALİZİ")
    analiz.append("-" * 40)
    analiz.append(f"  • {girinti_analiz}")
    analiz.append("")

    ziyaretci = FonksiyonZiyaretci()
    ziyaretci.visit(agac)
    
    # FONKSİYON İSTATİSTİKLERİ
    analiz.append("🧮 FONKSİYON İSTATİSTİKLERİ")
    analiz.append("-" * 40)
    analiz.append(f"  • Toplam Fonksiyon: {len(ziyaretci.fonksiyonlar)}")
    analiz.append(f"  • Toplam Sınıf: {len(ziyaretci.siniflar)}")
    analiz.append(f"  • İç İçe Fonksiyon: {len(ziyaretci.ic_ice_fonksiyonlar)}")
    analiz.append(f"  • Lambda Fonksiyon: {ziyaretci.lambda_sayaci}")
    analiz.append(f"  • Async Fonksiyon: {len(ziyaretci.async_fonksiyonlar)}")
    analiz.append(f"  • Walrus Operator: {ziyaretci.walrus_operator_sayaci}")
    analiz.append("")

    # TEKRARLAYAN KOD ANALİZİ
    tekrarlanan_fonksiyonlar = bul_tekrarlanan_kod(ziyaretci.fonksiyonlar)
    kod_klonlari = analiz_kod_klonlari(kaynak_kod)
    
    analiz.append("🔍 TEKRARLAYAN KOD ANALİZİ")
    analiz.append("-" * 40)
    if tekrarlanan_fonksiyonlar:
        analiz.append("  • Benzer Fonksiyon Çiftleri:")
        for fonk1, fonk2 in tekrarlanan_fonksiyonlar[:5]:
            analiz.append(f"    • {fonk1} ↔ {fonk2}")
        if len(tekrarlanan_fonksiyonlar) > 5:
            analiz.append(f"    • ... ve {len(tekrarlanan_fonksiyonlar) - 5} çift daha")
    else:
        analiz.append("  • ✅ Tekrarlayan fonksiyon bulunamadı")
    
    for klon in kod_klonlari:
        analiz.append(f"  • {klon}")
    analiz.append("")

    # İÇ İÇE FONKSİYON ANALİZİ
    if ziyaretci.ic_ice_fonksiyonlar:
        analiz.append("🔄 İÇ İÇE FONKSİYONLAR")
        analiz.append("-" * 40)
        for fonk, parent in ziyaretci.ic_ice_fonksiyonlar:
            analiz.append(f"  • {fonk.name} → {parent.name} içinde")
        analiz.append("  ⚠️ İç içe fonksiyonlar closure oluşturur, memory etkisi olabilir")
        analiz.append("")

    # YENİ ÖZELLİKLER - GENEL ANALİZ
    analiz.append("📊 GENEL KALİTE METRİKLERİ")
    analiz.append("-" * 40)
    
    # Kütüphane kullanım analizi
    kutuphane_puan, kutuphane_oneriler = kontrol_kutuphane_kullanimi(ziyaretci)
    analiz.append(f"  📦 Kütüphane Kullanımı: {kutuphane_puan}/10")
    for oneri in kutuphane_oneriler:
        analiz.append(f"    • {oneri}")
    analiz.append("")

    # FONKSİYON BAZLI DETAYLI ANALİZ
    toplam_puan = 0
    fonk_sayisi = len(ziyaretci.fonksiyonlar)
    
    if fonk_sayisi > 0:
        analiz.append("🧪 DETAYLI FONKSİYON ANALİZİ")
        analiz.append("=" * 80)
        
        for i, (fonk, parent) in enumerate(ziyaretci.fonksiyonlar, 1):
            analiz.append(f"🔹 FONKSİYON {i}: {fonk.name}")
            analiz.append("-" * 50)
            
            # Temel bilgiler
            parametreler = [arg.arg for arg in fonk.args.args]
            analiz.append(f"  📋 Parametreler: {', '.join(parametreler) if parametreler else 'Yok'}")
            
            # YENİ ÖZELLİKLER
            # Docstring kalitesi
            docstring_puan, docstring_oneriler = kontrol_docstring_kalitesi(fonk)
            analiz.append(f"  📝 Docstring Kalitesi: {docstring_puan}/10")
            for oneri in docstring_oneriler[:2]:
                analiz.append(f"    • {oneri}")
            
            # Type hint kapsamı
            typehint_puan, typehint_oneriler = kontrol_type_hint_kapsami(fonk)
            analiz.append(f"  🎯 Type Hint Kapsamı: {typehint_puan}/10")
            for oneri in typehint_oneriler[:2]:
                analiz.append(f"    • {oneri}")
            
            # Async uygunluk
            async_puan, async_oneriler = kontrol_async_uygunluk(fonk)
            analiz.append(f"  ⚡ Async Uygunluk: {async_puan}/5")
            for oneri in async_oneriler:
                analiz.append(f"    • {oneri}")
            
            # Zaman karmaşıklığı
            zaman_karmasikligi, zaman_analiz = analiz_zaman_karmasikligi(fonk)
            analiz.append(f"  ⏰ Zaman Karmaşıklığı: {zaman_karmasikligi}")
            
            # Code smells
            smell_puan, smell_analiz = kontrol_code_smells(fonk)
            analiz.append(f"  👃 Code Smells: {smell_puan}/10")
            for smell in smell_analiz:
                analiz.append(f"    • {smell}")
            
            # ÖNCEKİ ÖZELLİKLER (KORUNDU)
            # Döngü kompleksitesi
            dongu_puan, dongu_analiz = kontrol_dongu_kompleksitesi(fonk)
            analiz.append(f"  🔁 Döngü Kompleksitesi: {dongu_puan}/5")
            for analiz_item in dongu_analiz[:2]:
                analiz.append(f"    • {analiz_item}")
            
            # Try/Except analizi
            try_puan, try_analiz = kontrol_try_except_detayli(fonk)
            analiz.append(f"  🛡️  Try/Except Kalitesi: {try_puan}/5")
            for analiz_item in try_analiz[:2]:
                analiz.append(f"    • {analiz_item}")
            
            # Memory impact
            memory_puan, memory_analiz = kontrol_memory_impact(fonk)
            analiz.append(f"  💾 Memory Impact: {memory_puan}/5")
            for analiz_item in memory_analiz:
                analiz.append(f"    • {analiz_item}")
            
            # Riskli kodlar
            riskli_kodlar = []
            for n in ast.walk(fonk):
                if isinstance(n, ast.Call):
                    if isinstance(n.func, ast.Name) and n.func.id in RISKLI_IFADELER:
                        riskli_kodlar.append(n.func.id)
                    elif isinstance(n.func, ast.Attribute):
                        full_name = f"{ast.unparse(n.func.value)}.{n.func.attr}"
                        if full_name in RISKLI_IFADELER:
                            riskli_kodlar.append(full_name)
            
            analiz.append(f"  ⚠️  Riskli Kodlar: {', '.join(riskli_kodlar) if riskli_kodlar else 'Yok'}")
            
            # Toplam puan hesapla
            fonk_toplam_puan = (docstring_puan + typehint_puan + async_puan + 
                              dongu_puan + try_puan + smell_puan + memory_puan) / 7
            toplam_puan += fonk_toplam_puan
            
            analiz.append(f"  💯 Fonksiyon Toplam Puan: {fonk_toplam_puan:.1f}/10")
            analiz.append("")
            
    # GENEL DEĞERLENDİRME
    ortalama_puan = toplam_puan / fonk_sayisi if fonk_sayisi > 0 else 0
    
    analiz.append("🎓 GENEL DEĞERLENDİRME")
    analiz.append("=" * 80)
    analiz.append(f"📈 Ortalama Fonksiyon Kalite Puanı: {ortalama_puan:.1f}/5")
    
    if ortalama_puan >= 4:
        analiz.append("✅ MÜKEMMEL: Profesyonel seviyede kod kalitesi!")
    elif ortalama_puan >= 3:
        analiz.append("👍 İYİ: İyi başlangıç, küçük iyileştirmelerle mükemmel olabilir")
    elif ortalama_puan >= 2:
        analiz.append("⚠️ ORTA: Temel prensiplere odaklanılmalı")
    else:
        analiz.append("❌ GELİŞTİRME GEREKLİ: Temel seviye iyileştirmeler şart")
    
    # ÖZET RAPOR
    analiz.append("")
    analiz.append("📊 ÖZET RAPOR")
    analiz.append("-" * 40)
    analiz.append(f"  • Parantez Dengesi: {'✅' if parantez_dengesi else '❌'}")
    analiz.append(f"  • Tekrarlayan Kod: {'❌' if tekrarlanan_fonksiyonlar else '✅'} {len(tekrarlanan_fonksiyonlar)} çift")
    analiz.append(f"  • İç İçe Fonksiyon: {len(ziyaretci.ic_ice_fonksiyonlar)} adet")
    analiz.append(f"  • Lambda Kullanımı: {ziyaretci.lambda_sayaci} adet")
    
    # Toplam karmaşıklık hesapla
    toplam_karmaşıklık = 0
    for fonk, _ in ziyaretci.fonksiyonlar:
        toplam_karmaşıklık += sum(1 for n in ast.walk(fonk) 
                                if isinstance(n, (ast.If, ast.For, ast.While)))
    analiz.append(f"  • Toplam Karmaşıklık: {toplam_karmaşıklık}")

    # Dosyaya kaydet
    try:
        dosya_adi = Path(dosya_yolu).stem
        analiz_dizini = "/opt/tradebot/globalislemler/global_testler"
        os.makedirs(analiz_dizini, exist_ok=True)
        
        analiz_dosya = f"{dosya_adi}_dna_analiz.txt"
        analiz_yolu = os.path.join(analiz_dizini, analiz_dosya)
        
        with open(analiz_yolu, "w", encoding="utf-8") as f:
            f.write("\n".join(analiz))
        
        analiz.append("")
        analiz.append(f"💾 Detaylı analiz raporu kaydedildi: {analiz_yolu}")
        
    except Exception as e:
        analiz.append(f"❌ Dosyaya yazma hatası: {e}")

    return "\n".join(analiz)

def main():
    print("🧬 Python Kod DNA Analiz Aracı")
    print("=" * 50)
    print("Kodunuzun genetik yapısını analiz ediyoruz...")
    
    try:
        dosya_yolu = input("\n📂 Analiz edilecek dosya yolunu girin: ").strip()
        
        if not os.path.exists(dosya_yolu):
            print("❌ Dosya bulunamadı! Lütfen geçerli bir yol girin.")
            return
            
        print("\n⏳ Kod DNA'sı analiz ediliyor...")
        sonuc = analiz_yap(dosya_yolu)
        
        print("\n" + "=" * 80)
        print(sonuc)
        print("=" * 80)
        
    except KeyboardInterrupt:
        print("\n\n👋 Çıkış yapılıyor...")
    except Exception as e:
        print(f"❌ Beklenmeyen hata: {e}")

if __name__ == "__main__":
    main()