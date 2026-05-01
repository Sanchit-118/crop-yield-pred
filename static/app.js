const form = document.getElementById("prediction-form");
const fillExampleButton = document.getElementById("fill-example");
const datasetRowSelect = document.getElementById("dataset-row-select");
const recommendCropButton = document.getElementById("recommend-crop");
const resultBox = document.getElementById("result");
const historyBox = document.getElementById("history");
const validationBox = document.getElementById("validation-box");
const yieldChartContainer = document.getElementById("yield-chart-container");
const riskChartContainer = document.getElementById("risk-chart-container");
const trendChartContainer = document.getElementById("trend-chart-container");
const chartContextBanner = document.getElementById("chart-context-banner");
const userMenuToggle = document.getElementById("user-menu-toggle");
const userMenuDropdown = document.getElementById("user-menu-dropdown");
const advisoryToggle = document.getElementById("advisory-toggle");
const advisoryPopover = document.getElementById("advisory-popover");
const modelGuideToggle = document.getElementById("model-guide-toggle");
const modelGuideDrawer = document.getElementById("model-guide-drawer");
const modelGuideClose = document.getElementById("model-guide-close");
const modelGuideTabs = document.querySelectorAll("[data-guide-tab]");
const modelGuidePanels = document.querySelectorAll("[data-guide-panel]");
const workflowStepButtons = document.querySelectorAll("[data-workflow-step]");
const workflowDetailPill = document.getElementById("workflow-detail-pill");
const workflowDetailTitle = document.getElementById("workflow-detail-title");
const workflowDetailCopy = document.getElementById("workflow-detail-copy");
const workflowDetailWhy = document.getElementById("workflow-detail-why");
const workflowDetailNext = document.getElementById("workflow-detail-next");
const advisoryBadge = document.getElementById("advisory-badge");
const advisoryPopoverList = document.getElementById("advisory-popover-list");
const advisoryPopoverMore = document.getElementById("advisory-popover-more");
const notificationList = document.getElementById("notification-list");
const toggleAlertsViewButton = document.getElementById("toggle-alerts-view");
const refreshAdvisoriesButton = document.getElementById("refresh-advisories");
const advisoryPreferencesForm = document.getElementById("advisory-preferences-form");
const advisorySettingsFeedback = document.getElementById("advisory-settings-feedback");
const advisoryStatusPill = document.getElementById("advisory-status-pill");
const advisoryCategoryFilter = document.getElementById("advisory-category-filter");
const advisorySeverityFilter = document.getElementById("advisory-severity-filter");
const advisorySpotlight = document.getElementById("advisory-spotlight");
const advisorySpotlightClose = document.getElementById("advisory-spotlight-close");
const advisorySpotlightDismiss = document.getElementById("advisory-spotlight-dismiss");
const advisorySpotlightPriority = document.getElementById("advisory-spotlight-priority");
const advisorySpotlightTitle = document.getElementById("advisory-spotlight-title");
const advisorySpotlightMessage = document.getElementById("advisory-spotlight-message");
const advisorySpotlightNote = document.getElementById("advisory-spotlight-note");
const advisorySpotlightRecommendation = document.getElementById("advisory-spotlight-recommendation");
const advisorySpotlightLink = document.getElementById("advisory-spotlight-link");
const advisoryDecisionHero = document.getElementById("advisory-decision-hero");
const decisionHeroTitle = document.getElementById("decision-hero-title");
const decisionHeroSubtitle = document.getElementById("decision-hero-subtitle");
const decisionHeroMetrics = document.getElementById("decision-hero-metrics");
const decisionHeroDetails = document.getElementById("decision-hero-details");
const advisoryQuickInsights = document.getElementById("advisory-quick-insights");
const assistantFab = document.getElementById("assistant-fab");
const assistantChatbot = document.getElementById("assistant-chatbot");
const assistantClose = document.getElementById("assistant-close");
const compareCurrent = document.getElementById("compare-current");
const compareImproved = document.getElementById("compare-improved");
const compareScenariosButton = document.getElementById("compare-scenarios");
const downloadReportButton = document.getElementById("download-report");
const saveChartImageButton = document.getElementById("save-chart-image");
const datasetUploadForm = document.getElementById("dataset-upload-form");
const datasetFileInput = document.getElementById("dataset-file-input");
const datasetResetButton = document.getElementById("dataset-reset-button");
const datasetStatus = document.getElementById("dataset-status");
const togglePredictionExplainerButton = document.getElementById("toggle-prediction-explainer");
const resetFormButton = document.getElementById("reset-form");
const applyAssistantButton = document.getElementById("apply-assistant");
const assistantRain = document.getElementById("assistant-rain");
const assistantPest = document.getElementById("assistant-pest");
const assistantSoil = document.getElementById("assistant-soil");
const assistantFertilizer = document.getElementById("assistant-fertilizer");
const quickOpenAdvisoryButton = document.getElementById("quick-open-advisory");
const quickOpenHistoryButton = document.getElementById("quick-open-history");
const quickOpenDatasetButton = document.getElementById("quick-open-dataset");
const quickOpenContactButton = document.getElementById("quick-open-contact");
const languageSelect = document.getElementById("language-select");
const themeSelect = document.getElementById("theme-select");
const modeButtons = document.querySelectorAll(".mode-button");
const infoToggleButtons = document.querySelectorAll(".info-toggle-button");
const infoModal = document.getElementById("info-modal");
const infoModalClose = document.getElementById("info-modal-close");
const infoModalEyebrow = document.getElementById("info-modal-eyebrow");
const infoModalTitle = document.getElementById("info-modal-title");
const infoModalBody = document.getElementById("info-modal-body");
const simpleModePanel = document.getElementById("simple-mode");
const advancedModePanel = document.getElementById("advanced-mode");
const simpleCountry = document.getElementById("simple-country");
const simpleDirection = document.getElementById("simple-direction");
const simpleSeason = document.getElementById("simple-season");
const simpleCrop = document.getElementById("simple-crop");
const simpleRain = document.getElementById("simple-rain");
const simpleFertilizer = document.getElementById("simple-fertilizer");
const simplePest = document.getElementById("simple-pest");
const simpleClimateProfile = document.getElementById("simple-climate-profile");
const simplePreviewCard = document.getElementById("simple-preview-card");
let activeMode = "simple";
let currentLanguage = localStorage.getItem("cropAppLanguage") || "en";
let currentTheme = localStorage.getItem("cropAppTheme") || "light";
let lastPrediction = null;
let lastScenarioInput = null;
let predictionExplainerOpen = false;
let advisoryState = window.initialAdvisoryState || { notifications: [], preferences: {}, unread_count: 0, smtp_ready: false };
let advisoryFilters = { category: "all", severity: "all" };
let alertsExpanded = false;

const workflowGuideDetails = {
  input: {
    pill: "Step 01",
    title: "User Input",
    copy: "Field, climate, and soil values enter the prediction flow so the system starts from actual farm conditions instead of generic assumptions.",
    why: "Reliable advisories and predictions depend on good ground signals like rainfall, soil type, nutrients, and pest pressure.",
    next: "The app standardizes these values so each model can read the same profile consistently.",
  },
  prep: {
    pill: "Step 02",
    title: "Data Preprocessing",
    copy: "Raw values are aligned into the same structure used during model training, so live inputs behave like the training dataset.",
    why: "Without preprocessing, model output becomes less stable and comparisons across models become weaker.",
    next: "Once the input is normalized, the models can be evaluated against the same feature set.",
  },
  compare: {
    pill: "Step 03",
    title: "Model Comparison",
    copy: "Each regression model is evaluated on validation metrics to see which one handles the current agricultural patterns best.",
    why: "Different models perform differently depending on whether the data is more linear, mixed, or strongly nonlinear.",
    next: "The strongest performer is promoted to the live prediction role for this dataset.",
  },
  select: {
    pill: "Step 04",
    title: "Best Model Selection",
    copy: "The system promotes the model with the best balance of R2, RMSE, and MAE into the live prediction engine.",
    why: "This keeps the user-facing result tied to the most trustworthy validation performer instead of a fixed hardcoded model.",
    next: "That selected model is then used to generate the expected crop yield for the user input.",
  },
  yield: {
    pill: "Step 05",
    title: "Yield Prediction",
    copy: "The selected live model estimates the expected crop yield under the current combination of climate, soil, and nutrient conditions.",
    why: "This is the core forecast users depend on for planning crop decisions and field management.",
    next: "The predicted yield is then paired with a separate risk interpretation layer.",
  },
  risk: {
    pill: "Step 06",
    title: "Fuzzy Risk Evaluation",
    copy: "Risk logic interprets uncertainty around rainfall, stress, soil condition, and pests to make the final result easier to trust.",
    why: "A good predicted yield is not enough on its own if the surrounding field conditions are unstable or risky.",
    next: "The user receives a prediction outcome that is not only accurate, but also easier to act on in real conditions.",
  },
};

const helperModalContent = {
  "yield-unit": {
    eyebrow: "Yield Unit",
    title: "What does t/ha actually mean?",
    body: [
      "<p><strong>t/ha means ton per hectare.</strong> It tells you how much crop output is expected from one hectare of land.</p>",
      "<p>If the result is <strong>5.9 t/ha</strong>, the model is estimating about <strong>5.9 metric tons of crop from 1 hectare</strong> under similar field conditions.</p>",
    ],
  },
  "why-advanced": {
    eyebrow: "Why Advanced Mode",
    title: "Useful even for users who already know the inputs",
    body: [
      "<p>Advanced Mode is not just a form. It turns technical farm values into one decision view: predicted yield, risk score, confidence, crop recommendation, comparison, and analytics.</p>",
      "<p>So even if someone already knows rainfall, pH, NPK, and pest conditions, the website still saves them from manually interpreting all of those values separately.</p>",
    ],
  },
};

const translations = {
  en: {
    eyebrow_ai_agri: "AI + Agriculture",
    title_main: "Crop Yield Prediction System",
    language: "Language",
    theme: "Theme",
    dark_mode: "Dark Mode",
    light_mode: "Light Mode",
    header_pill: "Working Flask backend + live prediction API",
    logout: "Logout",
    project_goal: "Project Goal",
    hero_heading: "Predict crop yield from environmental and soil conditions.",
    hero_text: "This application trains regression models on a dataset, compares model performance, predicts yield from user input, and classifies overall agricultural risk using a weighted scoring method.",
    try_predictor: "Try Live Predictor",
    dataset_rows: "Dataset Rows",
    crops_covered: "Crops Covered",
    average_yield: "Average Yield",
    best_model: "Best Model",
    model_evaluation: "Model Evaluation",
    trained_compared: "Trained and compared on the dataset",
    live_prediction: "Live Prediction",
    prediction_subtitle: "Use Simple Mode for quick prediction or Advanced Mode for expert analysis",
    risk_formula: "Risk Score = Sum(Weight * Factor Score)",
    simple_mode: "Simple Mode",
    advanced_mode: "Advanced Mode",
    farmer_friendly: "Farmer-friendly input",
    simple_intro: "Choose location, season, crop, and current field conditions. The system will fill the technical profile automatically.",
    country: "Country",
    direction: "Direction",
    season: "Season",
    north: "North",
    south: "South",
    east: "East",
    west: "West",
    crop: "Crop",
    rain_situation: "Rain Situation",
    normal: "Normal",
    low_rain: "Low Rain",
    heavy_rain: "Heavy Rain",
    fertilizer_level: "Fertilizer Level",
    balanced: "Balanced",
    low: "Low",
    medium: "Medium",
    high: "High",
    pest_situation: "Pest Situation",
    autofilled_profile: "Auto-filled Technical Profile",
    select_dataset_record: "Select Dataset Record",
    use_live_input: "Use only live input",
    advanced_help: "Advanced Mode keeps the detailed scientific parameters for research, lab values, and comparative testing.",
    crop_type: "Crop Type",
    region: "Region",
    soil_type: "Soil Type",
    rainfall_mm: "Rainfall (mm)",
    temperature_c: "Temperature (C)",
    humidity_pct: "Humidity (%)",
    soil_ph: "Soil pH",
    nitrogen: "Nitrogen (kg/ha)",
    phosphorus: "Phosphorus (kg/ha)",
    potassium: "Potassium (kg/ha)",
    pest_risk: "Pest Risk (0-10)",
    predict_yield: "Predict Yield",
    recommend_best_crop: "Recommend Best Crop",
    load_example: "Load Example",
    prediction_output: "Prediction Output",
    result_placeholder: "Submit the form to calculate yield potential and risk.",
    prediction_history: "Prediction History",
    recent_predictions: "Your recent prediction results",
    history_empty: "Your prediction history will appear here.",
    dataset_preview: "Dataset Preview",
    example_records: "Example records used for training",
    no_profile: "No matching dataset profile found.",
    simple_mode_note: "Simple Mode uses dataset-based defaults so non-technical users can predict yield without entering lab values.",
    loading_predict: "Predicting... please wait",
    loading_recommend: "Finding best crop...",
    error_prediction: "Error in prediction",
    error_recommend: "Could not recommend a crop",
    history_load_error: "Could not load prediction history.",
    history_crop_unknown: "Unknown crop",
    history_risk_unknown: "Unknown",
    dataset_yield: "Dataset Yield",
    difference: "Difference",
    not_selected: "Not selected",
    model: "Model",
    risk_level: "Risk Level",
    score: "Score",
    recommended_crop: "Recommended Crop",
    merged_yield: "Merged Yield",
    rainfall_deviation: "Rainfall Deviation",
    temperature_stress: "Temperature Stress",
    soil_condition: "Soil Condition",
    recommendations: "Recommendations",
    best_crop: "Best Crop",
    match_score: "Match Score",
    all_crop_predictions: "All Crop Predictions",
    dataset_row_loaded: "Dataset row loaded",
    original_yield: "Original Yield",
    dataset_loaded_note: "Now run prediction or crop recommendation using this row as context.",
    load_dataset_error: "Could not load dataset row.",
    preview_country: "Country",
    preview_direction: "Direction",
    preview_season: "Season",
    preview_region: "Mapped Region",
    preview_soil: "Soil Type",
    preview_rainfall: "Rainfall",
    preview_temperature: "Temperature",
    preview_humidity: "Humidity",
    preview_soil_ph: "Soil pH",
    preview_pest: "Pest Risk",
    preview_location_note: "Location Note",
    preview_climate_logic: "Climate Logic",
    preview_season_logic: "Season Logic",
    language_label: "हिंदी",
    assistant_applied: "Assistant applied your current field conditions. You can predict immediately now.",
    compare_placeholder_current: "Run a prediction to capture the current scenario.",
    compare_placeholder_improved: "Use the compare button after prediction to test a better scenario.",
    chart_context_empty: "Last predicted for: no live prediction yet. The charts below will react to your latest input.",
    compare_requires_prediction: "Run a prediction or recommendation first so the app can compare scenarios.",
    compare_error: "Scenario comparison could not be generated.",
    compare_current_scenario: "Current field management",
    compare_improved_scenario: "Improved management plan",
    compare_baseline_profile: "Baseline field profile",
    compare_same_profile_improved: "Same crop profile with improved management inputs",
    compare_improved_profile: "Improved comparison profile",
    compare_season: "Season",
    recommendation_basis: "The system recommends the crop with the strongest overall fit under the current field conditions, then uses lower risk and projected yield to break ties.",
    advisory_empty: "No advisories yet. Use refresh to generate the first advisory set from your current data.",
    advisory_refreshing: "Refreshing advisory feed...",
    advisory_saved: "Advisory preferences saved.",
    advisory_save_error: "Could not save advisory preferences.",
    advisory_mark_read: "Mark read",
    advisory_open: "Open",
    advisory_email_ready: "Email delivery ready",
    advisory_email_pending: "In-app only until SMTP is configured",
    advisory_refresh_done: "Smart advisory engine refreshed.",
    advisory_refresh_failed: "Advisory refresh failed.",
    advisory_saving: "Saving advisory preferences...",
    advisory_popup_dismissed: "Advisory dismissed for now."
  },
  hi: {
    eyebrow_ai_agri: "एआई + कृषि",
    title_main: "फसल उपज पूर्वानुमान प्रणाली",
    language: "भाषा",
    theme: "थीम",
    dark_mode: "डार्क मोड",
    light_mode: "लाइट मोड",
    header_pill: "वर्किंग फ्लास्क बैकएंड + लाइव प्रेडिक्शन एपीआई",
    logout: "लॉगआउट",
    project_goal: "परियोजना उद्देश्य",
    hero_heading: "पर्यावरण और मिट्टी की स्थितियों से फसल उपज का अनुमान लगाएं।",
    hero_text: "यह एप्लिकेशन डेटासेट पर रिग्रेशन मॉडल ट्रेन करता है, उनके प्रदर्शन की तुलना करता है, उपयोगकर्ता इनपुट से उपज का अनुमान लगाता है और वेटेड स्कोरिंग विधि से कृषि जोखिम को वर्गीकृत करता है।",
    try_predictor: "लाइव प्रेडिक्टर आज़माएं",
    dataset_rows: "डेटासेट पंक्तियाँ",
    crops_covered: "कवर की गई फसलें",
    average_yield: "औसत उपज",
    best_model: "सर्वश्रेष्ठ मॉडल",
    model_evaluation: "मॉडल मूल्यांकन",
    trained_compared: "डेटासेट पर प्रशिक्षित और तुलना की गई",
    live_prediction: "लाइव पूर्वानुमान",
    prediction_subtitle: "त्वरित पूर्वानुमान के लिए सिंपल मोड या विशेषज्ञ विश्लेषण के लिए एडवांस्ड मोड का उपयोग करें",
    risk_formula: "जोखिम स्कोर = योग(वेट × फैक्टर स्कोर)",
    simple_mode: "सिंपल मोड",
    advanced_mode: "एडवांस्ड मोड",
    farmer_friendly: "किसान-अनुकूल इनपुट",
    simple_intro: "देश, दिशा और खेत की सामान्य स्थिति चुनें। सिस्टम डेटासेट आधारित क्षेत्रीय औसत और लोकेशन समायोजन से तकनीकी मान स्वतः भरेगा।",
    country: "देश",
    direction: "दिशा",
    north: "उत्तर",
    south: "दक्षिण",
    east: "पूर्व",
    west: "पश्चिम",
    crop: "फसल",
    rain_situation: "वर्षा स्थिति",
    normal: "सामान्य",
    low_rain: "कम वर्षा",
    heavy_rain: "अधिक वर्षा",
    fertilizer_level: "उर्वरक स्तर",
    balanced: "संतुलित",
    low: "कम",
    medium: "मध्यम",
    high: "उच्च",
    pest_situation: "कीट स्थिति",
    autofilled_profile: "स्वतः भरी तकनीकी प्रोफ़ाइल",
    select_dataset_record: "डेटासेट रिकॉर्ड चुनें",
    use_live_input: "केवल लाइव इनपुट उपयोग करें",
    advanced_help: "एडवांस्ड मोड रिसर्च, लैब वैल्यू और तुलना परीक्षण के लिए विस्तृत वैज्ञानिक पैरामीटर रखता है।",
    crop_type: "फसल प्रकार",
    region: "क्षेत्र",
    soil_type: "मिट्टी प्रकार",
    rainfall_mm: "वर्षा (मिमी)",
    temperature_c: "तापमान (C)",
    humidity_pct: "आर्द्रता (%)",
    soil_ph: "मिट्टी pH",
    nitrogen: "नाइट्रोजन (किग्रा/हेक्टेयर)",
    phosphorus: "फॉस्फोरस (किग्रा/हेक्टेयर)",
    potassium: "पोटैशियम (किग्रा/हेक्टेयर)",
    pest_risk: "कीट जोखिम (0-10)",
    predict_yield: "उपज का अनुमान लगाएं",
    recommend_best_crop: "सर्वश्रेष्ठ फसल सुझाएं",
    load_example: "उदाहरण लोड करें",
    prediction_output: "पूर्वानुमान परिणाम",
    result_placeholder: "उपज क्षमता और जोखिम निकालने के लिए फॉर्म सबमिट करें।",
    prediction_history: "पूर्वानुमान इतिहास",
    recent_predictions: "आपके हाल के पूर्वानुमान परिणाम",
    history_empty: "आपका पूर्वानुमान इतिहास यहां दिखाई देगा।",
    dataset_preview: "डेटासेट पूर्वावलोकन",
    example_records: "प्रशिक्षण के लिए उपयोग किए गए उदाहरण रिकॉर्ड",
    no_profile: "मिलती हुई डेटासेट प्रोफ़ाइल नहीं मिली।",
    simple_mode_note: "सिंपल मोड डेटासेट आधारित डिफ़ॉल्ट उपयोग करता है ताकि बिना लैब वैल्यू के भी पूर्वानुमान किया जा सके।",
    loading_predict: "पूर्वानुमान किया जा रहा है... कृपया प्रतीक्षा करें",
    loading_recommend: "सर्वश्रेष्ठ फसल खोजी जा रही है...",
    error_prediction: "पूर्वानुमान में त्रुटि",
    error_recommend: "फसल सुझाव नहीं मिल सका",
    history_load_error: "पूर्वानुमान इतिहास लोड नहीं हो सका।",
    history_crop_unknown: "अज्ञात फसल",
    history_risk_unknown: "अज्ञात",
    dataset_yield: "डेटासेट उपज",
    difference: "अंतर",
    not_selected: "चयनित नहीं",
    model: "मॉडल",
    risk_level: "जोखिम स्तर",
    score: "स्कोर",
    recommended_crop: "अनुशंसित फसल",
    merged_yield: "मर्ज उपज",
    rainfall_deviation: "वर्षा विचलन",
    temperature_stress: "तापमान तनाव",
    soil_condition: "मिट्टी की स्थिति",
    recommendations: "सुझाव",
    best_crop: "सर्वश्रेष्ठ फसल",
    match_score: "मैच स्कोर",
    all_crop_predictions: "सभी फसल परिणाम",
    dataset_row_loaded: "डेटासेट पंक्ति लोड हो गई",
    original_yield: "मूल उपज",
    dataset_loaded_note: "अब इस पंक्ति को संदर्भ बनाकर पूर्वानुमान या फसल सुझाव चलाएं।",
    load_dataset_error: "डेटासेट पंक्ति लोड नहीं हो सकी।",
    preview_country: "देश",
    preview_direction: "दिशा",
    preview_region: "मैप किया गया क्षेत्र",
    preview_soil: "मिट्टी प्रकार",
    preview_rainfall: "वर्षा",
    preview_temperature: "तापमान",
    preview_humidity: "आर्द्रता",
    preview_soil_ph: "मिट्टी pH",
    preview_pest: "कीट जोखिम",
    preview_location_note: "स्थान टिप्पणी",
    preview_climate_logic: "जलवायु तर्क",
    language_label: "English",
    assistant_applied: "असिस्टेंट ने आपकी मौजूदा खेत स्थिति लागू कर दी है। अब आप तुरंत पूर्वानुमान चला सकते हैं।",
    compare_placeholder_current: "मौजूदा स्थिति कैप्चर करने के लिए पहले एक पूर्वानुमान चलाएं।",
    compare_placeholder_improved: "पूर्वानुमान के बाद बेहतर स्थिति जांचने के लिए compare बटन इस्तेमाल करें।",
    chart_context_empty: "Last predicted for: no live prediction yet. The charts below will react to your latest input.",
    compare_requires_prediction: "तुलना करने से पहले एक पूर्वानुमान या recommendation चलाएं।",
    compare_error: "Scenario comparison generate नहीं हो सका।",
    compare_current_scenario: "मौजूदा खेत प्रबंधन",
    compare_improved_scenario: "सुधारित प्रबंधन योजना",
    compare_baseline_profile: "बेसलाइन फील्ड प्रोफाइल",
    compare_same_profile_improved: "उसी crop profile के साथ बेहतर management inputs",
    compare_improved_profile: "सुधारित comparison profile",
    compare_season: "Season",
    recommendation_basis: "सिस्टम मौजूदा खेत स्थिति में सबसे मजबूत overall fit वाली फसल को ऊपर रखता है, फिर lower risk और projected yield से tie break करता है।"
  }
};

translations.hinglish = {
  ...translations.en,
  eyebrow_ai_agri: "AI + Agriculture",
  title_main: "Crop Yield Prediction System",
  language: "Bhasha",
  theme: "Theme",
  dark_mode: "Dark Mode",
  light_mode: "Light Mode",
  logout: "Logout",
  project_goal: "Project Goal",
  hero_heading: "Environment aur soil conditions ke basis par crop yield predict karo.",
  hero_text: "Yeh application dataset par regression models train karta hai, unka performance compare karta hai, user input se yield predict karta hai, aur weighted scoring method se agricultural risk classify karta hai.",
  try_predictor: "Live Predictor Try Karo",
  dataset_rows: "Dataset Rows",
  crops_covered: "Crops Covered",
  average_yield: "Average Yield",
  best_model: "Best Model",
  model_evaluation: "Model Evaluation",
  season: "Season",
  trained_compared: "Dataset par train aur compare kiya gaya",
  live_prediction: "Live Prediction",
  prediction_subtitle: "Quick prediction ke liye Simple Mode, aur expert analysis ke liye Advanced Mode use karo",
  risk_formula: "Risk Score = Sum(Weight * Factor Score)",
  simple_mode: "Simple Mode",
  advanced_mode: "Advanced Mode",
  farmer_friendly: "Farmer-friendly input",
  simple_intro: "Location, season, crop, aur current field conditions select karo. System technical profile automatically fill karega.",
  country: "Country",
  direction: "Direction",
  north: "North",
  south: "South",
  east: "East",
  west: "West",
  crop: "Crop",
  rain_situation: "Rain Situation",
  normal: "Normal",
  low_rain: "Low Rain",
  heavy_rain: "Heavy Rain",
  fertilizer_level: "Fertilizer Level",
  balanced: "Balanced",
  low: "Low",
  medium: "Medium",
  high: "High",
  pest_situation: "Pest Situation",
  autofilled_profile: "Auto-filled Technical Profile",
  select_dataset_record: "Dataset Record Select Karo",
  use_live_input: "Sirf live input use karo",
  advanced_help: "Advanced Mode research, lab values, aur comparative testing ke liye detailed scientific parameters rakhta hai.",
  crop_type: "Crop Type",
  region: "Region",
  soil_type: "Soil Type",
  rainfall_mm: "Rainfall (mm)",
  temperature_c: "Temperature (C)",
  humidity_pct: "Humidity (%)",
  soil_ph: "Soil pH",
  nitrogen: "Nitrogen (kg/ha)",
  phosphorus: "Phosphorus (kg/ha)",
  potassium: "Potassium (kg/ha)",
  pest_risk: "Pest Risk (0-10)",
  predict_yield: "Predict Yield",
  recommend_best_crop: "Best Crop Recommend Karo",
  load_example: "Example Load Karo",
  prediction_output: "Prediction Output",
  result_placeholder: "Yield potential aur risk calculate karne ke liye form submit karo.",
  prediction_history: "Prediction History",
  recent_predictions: "Tumhare recent prediction results",
  history_empty: "Tumhari prediction history yahan dikhegi.",
  dataset_preview: "Dataset Preview",
  example_records: "Training ke liye use kiye gaye example records",
  no_profile: "Matching dataset profile nahi mili.",
  simple_mode_note: "Simple Mode dataset-based defaults use karta hai taaki non-technical users bina lab values ke bhi prediction kar sakein.",
  loading_predict: "Predict kar rahe hain... please wait",
  loading_recommend: "Best crop dhoondh rahe hain...",
  error_prediction: "Prediction mein error",
  error_recommend: "Crop recommend nahi ho saki",
  history_load_error: "Prediction history load nahi ho saki.",
  history_crop_unknown: "Unknown crop",
  history_risk_unknown: "Unknown",
  dataset_yield: "Dataset Yield",
  difference: "Difference",
  not_selected: "Not selected",
  model: "Model",
  risk_level: "Risk Level",
  score: "Score",
  recommended_crop: "Recommended Crop",
  merged_yield: "Merged Yield",
  rainfall_deviation: "Rainfall Deviation",
  temperature_stress: "Temperature Stress",
  soil_condition: "Soil Condition",
  recommendations: "Recommendations",
  best_crop: "Best Crop",
  match_score: "Match Score",
  all_crop_predictions: "All Crop Predictions",
  dataset_row_loaded: "Dataset row load ho gayi",
  original_yield: "Original Yield",
  dataset_loaded_note: "Ab is row ko context bana kar prediction ya crop recommendation chalao.",
  load_dataset_error: "Dataset row load nahi ho saki.",
  preview_country: "Country",
  preview_direction: "Direction",
  preview_region: "Mapped Region",
  preview_soil: "Soil Type",
  preview_rainfall: "Rainfall",
  preview_temperature: "Temperature",
  preview_humidity: "Humidity",
  preview_soil_ph: "Soil pH",
  preview_pest: "Pest Risk",
  preview_location_note: "Location Note",
  preview_climate_logic: "Climate Logic",
  language_label: "Hindi",
  assistant_applied: "Assistant ne current field conditions apply kar diye hain. Ab tum seedha prediction chala sakte ho.",
  compare_placeholder_current: "Current scenario capture karne ke liye pehle prediction chalao.",
  compare_placeholder_improved: "Prediction ke baad better scenario test karne ke liye compare button use karo.",
  chart_context_empty: "Last predicted for: no live prediction yet. The charts below will react to your latest input.",
  compare_requires_prediction: "Compare karne se pehle prediction ya recommendation chalao.",
  compare_error: "Scenario comparison generate nahi ho saka.",
  compare_current_scenario: "Current field management",
  compare_improved_scenario: "Improved management plan",
  compare_baseline_profile: "Baseline field profile",
  compare_same_profile_improved: "Same crop profile with improved management inputs",
  compare_improved_profile: "Improved comparison profile",
  compare_season: "Season",
  recommendation_basis: "System current field conditions ke under sabse strong overall fit wali crop ko top par rakhta hai, phir lower risk aur projected yield se tie break karta hai.",
};

function t(key) {
  return translations[currentLanguage]?.[key] || translations.en[key] || key;
}

function applyTheme(theme) {
  currentTheme = theme;
  document.body.classList.toggle("dark-mode", theme === "dark");
  localStorage.setItem("cropAppTheme", theme);
  if (themeSelect) {
    themeSelect.value = theme;
  }
  syncPlotlyTheme();
}

function getPlotlyThemeLayout() {
  const isDark = currentTheme === "dark";
  const fontColor = isDark ? "#eaf3ec" : "#243328";
  const mutedColor = isDark ? "#c6d6c8" : "#4e6252";
  const gridColor = isDark ? "rgba(214, 230, 214, 0.18)" : "rgba(67, 104, 80, 0.16)";
  const zeroLineColor = isDark ? "rgba(214, 230, 214, 0.24)" : "rgba(67, 104, 80, 0.22)";

  return {
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: {
      color: fontColor,
      family: "\"DM Sans\", sans-serif",
    },
    title: {
      x: 0.04,
      xanchor: "left",
      font: {
        color: fontColor,
        family: "\"Space Grotesk\", sans-serif",
        size: 22,
      },
    },
    legend: {
      font: {
        color: fontColor,
      },
      title: {
        font: {
          color: mutedColor,
        },
      },
    },
    xaxis: {
      color: mutedColor,
      gridcolor: gridColor,
      linecolor: zeroLineColor,
      zerolinecolor: zeroLineColor,
      title: {
        font: {
          color: fontColor,
        },
      },
      tickfont: {
        color: mutedColor,
      },
    },
    yaxis: {
      color: mutedColor,
      gridcolor: gridColor,
      linecolor: zeroLineColor,
      zerolinecolor: zeroLineColor,
      title: {
        font: {
          color: fontColor,
        },
      },
      tickfont: {
        color: mutedColor,
      },
    },
  };
}

function mergePlotlyLayout(baseLayout = {}, themeLayout = {}) {
  return {
    ...baseLayout,
    ...themeLayout,
    title: {
      ...(baseLayout.title || {}),
      ...(themeLayout.title || {}),
      font: {
        ...((baseLayout.title && baseLayout.title.font) || {}),
        ...((themeLayout.title && themeLayout.title.font) || {}),
      },
    },
    legend: {
      ...(baseLayout.legend || {}),
      ...(themeLayout.legend || {}),
      font: {
        ...((baseLayout.legend && baseLayout.legend.font) || {}),
        ...((themeLayout.legend && themeLayout.legend.font) || {}),
      },
      title: {
        ...((baseLayout.legend && baseLayout.legend.title) || {}),
        ...((themeLayout.legend && themeLayout.legend.title) || {}),
        font: {
          ...(((baseLayout.legend && baseLayout.legend.title) || {}).font || {}),
          ...(((themeLayout.legend && themeLayout.legend.title) || {}).font || {}),
        },
      },
    },
    xaxis: {
      ...(baseLayout.xaxis || {}),
      ...(themeLayout.xaxis || {}),
      title: {
        ...((baseLayout.xaxis && baseLayout.xaxis.title) || {}),
        ...((themeLayout.xaxis && themeLayout.xaxis.title) || {}),
        font: {
          ...(((baseLayout.xaxis && baseLayout.xaxis.title) || {}).font || {}),
          ...(((themeLayout.xaxis && themeLayout.xaxis.title) || {}).font || {}),
        },
      },
    },
    yaxis: {
      ...(baseLayout.yaxis || {}),
      ...(themeLayout.yaxis || {}),
      title: {
        ...((baseLayout.yaxis && baseLayout.yaxis.title) || {}),
        ...((themeLayout.yaxis && themeLayout.yaxis.title) || {}),
        font: {
          ...(((baseLayout.yaxis && baseLayout.yaxis.title) || {}).font || {}),
          ...(((themeLayout.yaxis && themeLayout.yaxis.title) || {}).font || {}),
        },
      },
    },
  };
}

function syncPlotlyTheme() {
  if (typeof Plotly === "undefined") {
    return;
  }

  const themedLayout = getPlotlyThemeLayout();
  document.querySelectorAll(".js-plotly-plot").forEach((chart) => {
    try {
      Plotly.relayout(chart, mergePlotlyLayout(chart.layout || {}, themedLayout));
    } catch (error) {
      console.warn("Could not sync chart theme", error);
    }
  });
}

function applyLanguage(language) {
  currentLanguage = language;
  localStorage.setItem("cropAppLanguage", language);

  document.documentElement.lang = language === "hi" ? "hi" : "en";
  document.querySelectorAll("[data-i18n]").forEach((element) => {
    const key = element.dataset.i18n;
    if (element.tagName === "OPTION") {
      element.textContent = t(key);
      return;
    }
    element.textContent = t(key);
  });

  if (languageSelect) {
    languageSelect.value = language;
  }
  if (themeSelect) {
    themeSelect.value = currentTheme;
  }

  renderSimplePreview();
}

function getFormData() {
  if (activeMode === "simple") {
    return getSimpleModePayload();
  }

  const formData = new FormData(form);
  const payload = {};
  formData.forEach((value, key) => {
    payload[key] = ["crop_type", "region", "soil_type", "season"].includes(key) ? value : Number(value);
  });
  return payload;
}

function getProfile(crop, region) {
  const cropProfiles = window.simpleProfiles?.[crop];
  if (cropProfiles?.[region]) {
    return cropProfiles[region];
  }

  const fallbackRegion = cropProfiles ? Object.keys(cropProfiles)[0] : null;
  return fallbackRegion ? cropProfiles[fallbackRegion] : null;
}

function getLocationProfile(country, direction) {
  const countryProfile = window.locationProfiles?.[country];
  if (!countryProfile) {
    return null;
  }

  const selectedDirection = countryProfile.directions?.[direction];
  if (selectedDirection) {
    return {
      country,
      direction,
      description: countryProfile.description,
      adjustments: countryProfile.adjustments || {},
      ...selectedDirection,
    };
  }

  const fallbackDirection = Object.keys(countryProfile.directions || {})[0];
  if (!fallbackDirection) {
    return null;
  }

  return {
    country,
    direction: fallbackDirection,
    description: countryProfile.description,
    adjustments: countryProfile.adjustments || {},
    ...countryProfile.directions[fallbackDirection],
  };
}

function buildSimpleProfile() {
  const locationProfile = getLocationProfile(simpleCountry.value, simpleDirection.value);
  if (!locationProfile) {
    return null;
  }

  const profile = getProfile(simpleCrop.value, locationProfile.region);
  if (!profile) {
    return null;
  }

  const adjusted = { ...profile };
  const seasonProfile = window.seasonProfiles?.[simpleSeason?.value || "Kharif"];
  adjusted.country = locationProfile.country;
  adjusted.direction = locationProfile.direction;
  adjusted.season = simpleSeason?.value || "Kharif";
  adjusted.location_summary = locationProfile.summary;
  adjusted.location_description = locationProfile.description;
  adjusted.region = locationProfile.region;

  const countryAdjustments = locationProfile.adjustments || {};
  adjusted.rainfall_mm = Number(adjusted.rainfall_mm) + Number(countryAdjustments.rainfall_mm || 0);
  adjusted.temperature_c = Number(adjusted.temperature_c) + Number(countryAdjustments.temperature_c || 0);
  adjusted.humidity_pct = Number(adjusted.humidity_pct) + Number(countryAdjustments.humidity_pct || 0);
  adjusted.soil_ph = Number(adjusted.soil_ph) + Number(countryAdjustments.soil_ph || 0);
  adjusted.nitrogen_kg_ha = Number(adjusted.nitrogen_kg_ha) + Number(countryAdjustments.nitrogen_kg_ha || 0);
  adjusted.phosphorus_kg_ha = Number(adjusted.phosphorus_kg_ha) + Number(countryAdjustments.phosphorus_kg_ha || 0);
  adjusted.potassium_kg_ha = Number(adjusted.potassium_kg_ha) + Number(countryAdjustments.potassium_kg_ha || 0);

  const rainAdjustment = { low: -140, normal: 0, high: 140 };
  adjusted.rainfall_mm = Number(adjusted.rainfall_mm) + rainAdjustment[simpleRain.value];

  const fertilizerAdjustment = { low: -18, medium: 0, high: 18 };
  const nutrientShift = fertilizerAdjustment[simpleFertilizer.value];
  adjusted.nitrogen_kg_ha = Number(adjusted.nitrogen_kg_ha) + nutrientShift;
  adjusted.phosphorus_kg_ha = Number(adjusted.phosphorus_kg_ha) + nutrientShift * 0.6;
  adjusted.potassium_kg_ha = Number(adjusted.potassium_kg_ha) + nutrientShift * 0.7;

  const pestMap = { low: 2.5, medium: 5.0, high: 7.5 };
  adjusted.pest_risk = pestMap[simplePest.value];

  if (seasonProfile) {
    adjusted.rainfall_mm = Number(adjusted.rainfall_mm) * Number(seasonProfile.rainfall_factor || 1);
    adjusted.temperature_c = Number(adjusted.temperature_c) + Number(seasonProfile.temperature_offset || 0);
    adjusted.humidity_pct = Number(adjusted.humidity_pct) * Number(seasonProfile.humidity_factor || 1);
    adjusted.nitrogen_kg_ha = Number(adjusted.nitrogen_kg_ha) * Number(seasonProfile.nutrient_factor || 1);
    adjusted.phosphorus_kg_ha = Number(adjusted.phosphorus_kg_ha) * Number(seasonProfile.nutrient_factor || 1);
    adjusted.potassium_kg_ha = Number(adjusted.potassium_kg_ha) * Number(seasonProfile.nutrient_factor || 1);
    adjusted.pest_risk = Math.max(0, Math.min(10, Number(adjusted.pest_risk) + Number(seasonProfile.pest_offset || 0)));
    adjusted.season_summary = seasonProfile.summary;
    adjusted.season_direction_profile = `${adjusted.direction} ${adjusted.season}`;
  }

  adjusted.humidity_pct = Math.max(25, Math.min(95, Number(adjusted.humidity_pct)));

  return adjusted;
}

function getSimpleModePayload() {
  const profile = buildSimpleProfile();
  if (!profile) {
    return {};
  }

  return {
    dataset_row_id: "",
    crop_type: profile.crop_type,
    region: profile.region,
    soil_type: profile.soil_type,
    country: profile.country,
    direction: profile.direction,
    season: profile.season,
    rainfall_mm: Number(profile.rainfall_mm),
    temperature_c: Number(profile.temperature_c),
    humidity_pct: Number(profile.humidity_pct),
    soil_ph: Number(profile.soil_ph),
    nitrogen_kg_ha: Number(profile.nitrogen_kg_ha),
    phosphorus_kg_ha: Number(profile.phosphorus_kg_ha),
    potassium_kg_ha: Number(profile.potassium_kg_ha),
    pest_risk: Number(profile.pest_risk),
    simple_mode_context: {
      country: simpleCountry.value,
      direction: simpleDirection.value,
      season: simpleSeason?.value || "Kharif",
      rain_situation: simpleRain.value,
      fertilizer_level: simpleFertilizer.value,
      pest_situation: simplePest.value,
    },
  };
}

function renderSimplePreview() {
  if (!simplePreviewCard) {
    return;
  }

  const profile = buildSimpleProfile();
  if (!profile) {
    if (simpleClimateProfile) {
      simpleClimateProfile.innerHTML = "";
    }
    simplePreviewCard.innerHTML = `<div>${t("no_profile")}</div>`;
    return;
  }

  if (simpleClimateProfile) {
    const rainLabel = simpleRain?.options?.[simpleRain.selectedIndex]?.text || "Normal";
    const fertilizerLabel = simpleFertilizer?.options?.[simpleFertilizer.selectedIndex]?.text || "Balanced";
    const pestLabel = simplePest?.options?.[simplePest.selectedIndex]?.text || "Low";

    simpleClimateProfile.innerHTML = `
      <div class="simple-profile-banner">
        <div class="simple-profile-copy">
          <p class="eyebrow">Agro-Climate Profile</p>
          <h4>${profile.direction} • ${profile.season}</h4>
          <p>${profile.location_summary}</p>
          <p class="profile-logic">${profile.season_summary || "Season profile not applied."}</p>
        </div>
        <div class="simple-profile-pills">
          <span>Crop: ${profile.crop_type}</span>
          <span>Water: ${rainLabel}</span>
          <span>Nutrients: ${fertilizerLabel}</span>
          <span>Pest Pressure: ${pestLabel}</span>
        </div>
      </div>
    `;
  }

    simplePreviewCard.innerHTML = `
      <div><span>${t("preview_soil")}</span><strong>${profile.soil_type}</strong></div>
      <div><span>${t("preview_rainfall")}</span><strong>${Math.round(profile.rainfall_mm)} mm</strong></div>
      <div><span>${t("preview_temperature")}</span><strong>${Math.round(profile.temperature_c)} C</strong></div>
      <div><span>${t("preview_humidity")}</span><strong>${Math.round(profile.humidity_pct)} %</strong></div>
      <div><span>${t("preview_soil_ph")}</span><strong>${profile.soil_ph}</strong></div>
      <div><span>${t("preview_pest")}</span><strong>${profile.pest_risk}</strong></div>
      <div class="preview-wide"><span>${t("preview_location_note")}</span><strong>${profile.location_summary}</strong></div>
      <div class="preview-wide"><span>${t("preview_season_logic")}</span><strong>${profile.season_summary || "Season profile not applied."}</strong></div>
      <div class="preview-wide"><span>${t("preview_climate_logic")}</span><strong>${profile.location_description}</strong></div>
    `;
}

function setMode(mode) {
  activeMode = mode;
  modeButtons.forEach((button) => {
    button.classList.toggle("is-active", button.dataset.mode === mode);
  });

  simpleModePanel.classList.toggle("is-active", mode === "simple");
  advancedModePanel.classList.toggle("is-active", mode === "advanced");

  if (mode === "simple") {
    renderSimplePreview();
    resultBox.innerHTML = `<div class="result-placeholder">${t("simple_mode_note")}</div>`;
  }
}

function openInfoModal(infoKey) {
  const content = helperModalContent[infoKey];
  if (!content || !infoModal || !infoModalTitle || !infoModalBody || !infoModalEyebrow) {
    return;
  }

  infoModalEyebrow.textContent = content.eyebrow;
  infoModalTitle.textContent = content.title;
  infoModalBody.innerHTML = content.body.join("");
  infoModal.classList.add("is-open");
  infoModal.setAttribute("aria-hidden", "false");
}

function closeInfoModal() {
  if (!infoModal) {
    return;
  }

  infoModal.classList.remove("is-open");
  infoModal.setAttribute("aria-hidden", "true");
}

function populateForm(values) {
  Object.entries(values).forEach(([key, value]) => {
    const field = form.elements.namedItem(key);
    if (field) {
      field.value = value;
    }
  });
}

function showLoading(message) {
  resultBox.innerHTML = `
    <div class="loading-state">
      <div class="spinner"></div>
      <p>${message}</p>
    </div>
  `;
}

function showError(message) {
  resultBox.innerHTML = `<div class="result-error">${message}</div>`;
}

function showValidationErrors(errors = []) {
  if (!validationBox) {
    return;
  }

  if (!errors.length) {
    validationBox.innerHTML = "";
    validationBox.classList.remove("is-visible");
    return;
  }

  validationBox.innerHTML = `<ul>${errors.map((item) => `<li>${item}</li>`).join("")}</ul>`;
  validationBox.classList.add("is-visible");
}

function updateChartContext(payload, predictionData = null) {
  if (!chartContextBanner) {
    return;
  }

  const crop = payload?.crop_type || "Unknown crop";
  const region = payload?.region || "Unknown region";
  const season = payload?.season ? ` during ${payload.season}` : "";
  const yieldText = predictionData?.predicted_yield ? ` Predicted yield: ${predictionData.predicted_yield} ton/hectare.` : "";
  chartContextBanner.textContent = `Last predicted for: ${crop} in ${region}${season}.${yieldText} Charts are now aligned with the latest input.`;
}

function buildImprovedScenario(payload) {
  return { ...payload };
}

function renderCompareCards(result) {
  if (!compareCurrent || !compareImproved) {
    return;
  }

  const yieldGain = Number(result.yield_gain || 0);
  const riskReduction = Number(result.risk_reduction || 0);
  const riskClassChanged = result.current.risk.level !== result.improved.risk.level;
  const compareSummary = riskClassChanged
    ? `↑ ${yieldGain.toFixed(3)} t/ha better yield | Risk moved from ${result.current.risk.level} to ${result.improved.risk.level}`
    : `↑ ${yieldGain.toFixed(3)} t/ha better yield | ↓ ${riskReduction.toFixed(1)} risk score`;
  const improvementNotes = (result.improvement_notes || [])
    .slice(0, 3)
    .map((item) => `<li>${item}</li>`)
    .join("");
  const sameProfile =
    result.current.crop_type === result.improved.crop_type &&
    result.current.region === result.improved.region &&
    result.current.season === result.improved.season;

  compareCurrent.innerHTML = `
    <h4>${result.current.crop_type}</h4>
    <p>${result.current.region}</p>
    ${result.current.season ? `<span class="compare-metric">${t("compare_season")}: ${result.current.season}</span>` : ""}
    <strong>${result.current.predicted_yield} ton/hectare</strong>
    <span class="compare-metric">Risk: ${result.current.risk.level}</span>
    <span class="compare-metric">Confidence: ${result.current.confidence.score}%</span>
    <span class="compare-metric">Scenario: ${t("compare_current_scenario")}</span>
    <span class="compare-profile-note">${t("compare_baseline_profile")}</span>
  `;
  compareImproved.innerHTML = `
    <h4>${result.improved.crop_type}</h4>
    <p>${result.improved.region}</p>
    ${result.improved.season ? `<span class="compare-metric">${t("compare_season")}: ${result.improved.season}</span>` : ""}
    <strong>${result.improved.predicted_yield} ton/hectare</strong>
    <span class="compare-metric">Risk: ${result.improved.risk.level}</span>
    <span class="compare-metric">Confidence: ${result.improved.confidence.score}%</span>
    <span class="compare-metric">Scenario: ${t("compare_improved_scenario")}</span>
    <span class="compare-profile-note">${sameProfile ? t("compare_same_profile_improved") : t("compare_improved_profile")}</span>
    <div class="compare-delta-row">
      <span class="compare-delta positive">↑ ${yieldGain.toFixed(3)} t/ha</span>
      <span class="compare-delta ${riskReduction >= 0 ? "positive" : "negative"}">${riskReduction >= 0 ? "↓" : "↑"} ${Math.abs(riskReduction).toFixed(1)} risk score</span>
    </div>
    <span class="compare-highlight">${compareSummary}</span>
    ${improvementNotes ? `<ul class="compare-notes">${improvementNotes}</ul>` : ""}
  `;
}

function renderPlotlyFigure(container, figure, chartKey) {
  if (!container || !figure || typeof Plotly === "undefined") {
    return;
  }

  let plotNode = container.querySelector(`[data-chart-key="${chartKey}"]`);
  if (!plotNode) {
    container.innerHTML = `<div class="dynamic-chart" data-chart-key="${chartKey}"></div>`;
    plotNode = container.querySelector(`[data-chart-key="${chartKey}"]`);
  }

  Plotly.react(plotNode, figure.data || [], mergePlotlyLayout(figure.layout || {}, getPlotlyThemeLayout()), {
    responsive: true,
    displayModeBar: false,
  });
}

function buildRiskReasonText(risk) {
  const factorEntries = [
    { key: "rainfall_deviation", label: "rainfall deviation", value: Number(risk?.factors?.rainfall_deviation || 0) },
    { key: "temperature_stress", label: "temperature deviation", value: Number(risk?.factors?.temperature_stress || 0) },
    { key: "soil_condition", label: "soil condition", value: Number(risk?.factors?.soil_condition || 0) },
    { key: "pest_risk", label: "pest risk", value: Number(risk?.factors?.pest_risk || 0) },
  ].sort((a, b) => b.value - a.value);

  const importantFactors = factorEntries.filter((item) => item.value >= 0.18).slice(0, 2);

  if (!importantFactors.length) {
    return "Risk explanation: Conditions are currently stable, with no major stress factor dominating the score.";
  }

  const factorText = importantFactors.map((item) => item.label).join(" and ");
  return `Risk explanation: The current score is mainly influenced by ${factorText}.`;
}

function buildMembershipRows(risk) {
  const memberships = risk?.fuzzy_factors || {};
  const labels = [
    { key: "rainfall_deviation", label: "Rainfall" },
    { key: "temperature_stress", label: "Temperature" },
    { key: "soil_condition", label: "Soil" },
    { key: "pest_risk", label: "Pest" },
  ];
  return labels
    .map((item) => {
      const value = Math.max(0, Math.min(1, Number(memberships[item.key] || 0)));
      return `
        <div class="membership-row">
          <span>${item.label}</span>
          <div class="membership-track"><div class="membership-fill" style="width:${value * 100}%"></div></div>
          <strong>${value.toFixed(2)}</strong>
        </div>
      `;
    })
    .join("");
}

async function updateContextCharts(payload, predictionData = null) {
  if (!yieldChartContainer || !riskChartContainer || !trendChartContainer) {
    return;
  }

  const response = await fetch("/api/context-figures", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      current_input: payload,
      predicted_yield: predictionData?.predicted_yield ?? null,
      risk: predictionData?.risk ?? null,
    }),
  });

  if (!response.ok) {
    return;
  }

  const figures = await response.json();
  renderPlotlyFigure(yieldChartContainer, figures.yield_by_crop, "yield");
  renderPlotlyFigure(riskChartContainer, figures.risk_distribution, "risk");
  renderPlotlyFigure(trendChartContainer, figures.trend, "trend");
}

function renderPredictionResult(data) {
  const recommendationItems = data.recommendations.map((item) => `<li>${item}</li>`).join("");
  const insightItems = (data.insights || []).map((item) => `<li>${item}</li>`).join("");
  const actionCards = (data.action_cards || [])
    .map(
      (card) => `
        <article class="action-card ${card.tone}">
          <span class="action-icon">${card.icon}</span>
          <div>
            <h5>${card.title}</h5>
            <p>${card.detail}</p>
          </div>
        </article>
      `
    )
    .join("");
  const riskReasonText = buildRiskReasonText(data.risk);
  const membershipRows = buildMembershipRows(data.risk);
  const seasonContext = data.season_message
    ? `<div class="season-context-banner">${data.season_message}</div>`
    : "";
  const gaAnnImprovement = data.ga_ann_improvement
    ? `
      <div class="ga-ann-box">
        <h4>GA + ANN Optimization</h4>
        <p><strong>ANN RMSE:</strong> ${data.ga_ann_improvement.baseline_rmse}</p>
        <p><strong>GA+ANN RMSE:</strong> ${data.ga_ann_improvement.optimized_rmse}</p>
        <p><strong>Improved by:</strong> ${data.ga_ann_improvement.rmse_gain_pct}%</p>
      </div>
    `
    : "";
  const predictionStages = `
      <div class="prediction-stage-list ${predictionExplainerOpen ? "is-open" : ""}" id="prediction-stage-list">
        <h4>How this prediction was built</h4>
        <div class="prediction-stage">
          <strong>Stage 1: Input profile</strong>
          <p>You selected ${data.input_summary?.crop_type || "the crop profile"} conditions, and the system prepared rainfall, temperature, soil, nutrient, and pest values for analysis.</p>
        </div>
        <div class="prediction-stage">
          <strong>Stage 2: Model estimate</strong>
          <p>${data.best_model} compared your input with patterns learned from the training dataset and estimated likely output.</p>
        </div>
        <div class="prediction-stage">
          <strong>Stage 3: Yield meaning</strong>
          <p>${data.predicted_yield} ton/hectare means about ${data.predicted_yield} metric tons of crop from 1 hectare of land under similar conditions.</p>
        </div>
        <div class="prediction-stage">
          <strong>Stage 4: Risk and advice</strong>
          <p>The risk score combines rainfall, temperature, soil, and pest pressure so the app can show field condition quality and generate recommendations.</p>
        </div>
      </div>
  `;

  resultBox.innerHTML = `
      <div class="result-summary result-decision-card reveal-in">
        <div class="insight-banner">${data.insights?.[0] || "Prediction prepared successfully."}</div>
        ${seasonContext}
        <div class="result-headline">
          <div>
            <p class="decision-label">FINAL AI DECISION</p>
            <h2>${data.predicted_yield} ton/hectare</h2>
            <span class="risk-badge ${String(data.risk.level).toLowerCase()}">${data.risk.level}</span>
          </div>
          <div class="confidence-meter">
            <span>Confidence</span>
            <div class="confidence-track"><div class="confidence-fill" style="width:${data.confidence.score}%"></div></div>
            <strong>${data.confidence.score}%</strong>
          </div>
        </div>
        <div class="ai-statement-box feature-panel feature-panel-ai">
          <span class="feature-kicker">AI Summary</span>
          <p>${data.ai_statement}</p>
        </div>
        <div class="decision-meta-grid">
          <section class="decision-stat-card emphasis-card">
            <span class="feature-kicker">Model Decision</span>
            <p><b>${t("model")}:</b> ${data.best_model}</p>
            <p><b>Best model metrics:</b> R2 ${data.best_model_metrics.r2} | RMSE ${data.best_model_metrics.rmse} | MAE ${data.best_model_metrics.mae}</p>
            <p><b>Risk engine:</b> ${data.risk.engine || "Fuzzy Logic Risk Engine"}</p>
          </section>
          <section class="decision-stat-card emphasis-card">
            <span class="feature-kicker">Risk Reading</span>
            <p><b>${t("risk_level")}:</b> ${data.risk.level}</p>
            <p><b>Overall Risk Score:</b> ${data.risk.score} / 100</p>
            <p><b>Risk explanation:</b> ${riskReasonText.replace("Risk explanation: ", "")}</p>
          </section>
            <section class="decision-stat-card emphasis-card">
              <span class="feature-kicker">Crop Fit</span>
              ${data.input_summary?.season ? `<p><b>Season context:</b> ${data.input_summary.season}</p>` : ""}
              <p><b>${t("recommended_crop")}:</b> ${data.recommended_crop}</p>
              <p><b>Interpretation:</b> ${data.predicted_yield} ton/hectare under similar conditions.</p>
            </section>
        </div>
        <div class="decision-detail-grid">
          <div class="decision-detail-column">
            <button type="button" class="button secondary explain-inline-button" onclick="window.__togglePredictionExplainer && window.__togglePredictionExplainer()">How this prediction was built</button>
            <div class="fuzzy-membership-box feature-panel">
              <span class="feature-kicker">Fuzzy Membership View</span>
              ${membershipRows}
            </div>
            <div class="factor-grid">
              <p><b>${t("rainfall_deviation")}:</b> ${data.risk.factors.rainfall_deviation}</p>
              <p><b>${t("temperature_stress")}:</b> ${data.risk.factors.temperature_stress}</p>
              <p><b>${t("soil_condition")}:</b> ${data.risk.factors.soil_condition}</p>
              <p><b>${t("pest_risk")}:</b> ${data.risk.factors.pest_risk}</p>
            </div>
            ${gaAnnImprovement}
          </div>
          <div class="decision-detail-column">
            <div class="action-grid">${actionCards}</div>
            <div class="insight-list feature-panel">
              <span class="feature-kicker">Field Insights</span>
              <ul>${insightItems}</ul>
            </div>
            <div class="insight-list feature-panel">
              <span class="feature-kicker">${t("recommendations")}</span>
              <ul>${recommendationItems}</ul>
            </div>
          </div>
        </div>
        ${predictionStages}
      </div>
    `;
}

function renderRecommendationResult(data) {
  const fitTone = (label) => {
    if (label === "Excellent fit") return "is-excellent";
    if (label === "Good fit") return "is-good";
    if (label === "Moderate fit") return "is-moderate";
    return "is-weak";
  };

  const items = data.all_predictions
    .map(
      (item, index) => `
        <article class="recommend-card ${index === 0 ? "top" : ""} ${fitTone(item.fit_label)}">
          <div>
            <h5>${index + 1}. ${item.crop}</h5>
            <p><span class="fit-pill ${fitTone(item.fit_label)}">${item.fit_label}</span> ${item.predicted_yield} t/ha | ${item.risk_level} risk</p>
          </div>
          <strong>${item.match_score}</strong>
        </article>
      `
    )
    .join("");
  const guide = data.score_guide
    .map((item) => `<li><strong>${item.label}</strong> (${item.range}): ${item.meaning}</li>`)
    .join("");

  resultBox.innerHTML = `
    <div class="result-summary recommendation-mode">
      <h2>${t("best_crop")}: ${data.recommended_crop}</h2>
      <p><b>Expected Yield:</b> ${data.expected_yield} ton/hectare</p>
      <p><b>Projected Risk:</b> ${data.risk_level} (${data.risk_score} / 100)</p>
      <p><b>${t("match_score")}:</b> ${data.match_score}</p>
      <p><b>Interpretation:</b> ${data.fit_label}</p>
      <p><b>Recommendation basis:</b> ${t("recommendation_basis")}</p>
      <div class="insight-banner">${data.score_note}</div>
      <div class="recommend-card-list">${items}</div>
      <div class="insight-list">
        <h4>${t("all_crop_predictions")}</h4>
        <ul>${guide}</ul>
      </div>
    </div>
  `;
}

async function submitPrediction(event) {
  event.preventDefault();
  const payload = getFormData();
  const validationErrors = [];
  if (!payload.crop_type) validationErrors.push("Please choose a crop.");
  if (!payload.region) validationErrors.push("Please choose a region.");
  if (Number(payload.humidity_pct) < 0 || Number(payload.humidity_pct) > 100) validationErrors.push("Humidity should be between 0 and 100.");
  if (Number(payload.pest_risk) < 0 || Number(payload.pest_risk) > 10) validationErrors.push("Pest risk should be between 0 and 10.");
  showValidationErrors(validationErrors);
  if (validationErrors.length) {
    showError("Please fix the input issues before predicting.");
    return;
  }

  showLoading(t("loading_predict"));

  const response = await fetch("/api/predict", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    showValidationErrors(errorData.validation_errors || []);
    showError(t("error_prediction"));
    return;
  }

  const result = await response.json();
  lastPrediction = { payload, result };
  lastScenarioInput = payload;
  showValidationErrors([]);
  renderPredictionResult(result);
  updateContextCharts(payload, result);
  updateChartContext(payload, result);
  loadHistory();
}

async function recommendCrop() {
  const payload = getFormData();
  const validationErrors = [];
  if (!payload.crop_type) validationErrors.push("Please choose a crop.");
  if (!payload.region) validationErrors.push("Please choose a region.");
  if (Number(payload.humidity_pct) < 0 || Number(payload.humidity_pct) > 100) validationErrors.push("Humidity should be between 0 and 100.");
  if (Number(payload.pest_risk) < 0 || Number(payload.pest_risk) > 10) validationErrors.push("Pest risk should be between 0 and 10.");
  showValidationErrors(validationErrors);
  if (validationErrors.length) {
    showError("Please fix the input issues before requesting a recommendation.");
    return;
  }
  showLoading(t("loading_recommend"));

  const response = await fetch("/api/recommend-crop", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    showValidationErrors(errorData.validation_errors || []);
    showError(t("error_recommend"));
    return;
  }

  const result = await response.json();
  lastScenarioInput = payload;
  renderRecommendationResult(result);
  updateContextCharts(payload);
  updateChartContext(payload);
}

async function loadHistory() {
  if (!historyBox) {
    return;
  }
  const response = await fetch("/api/history");
  if (!response.ok) {
    historyBox.innerHTML = `<div class="history-empty">${t("history_load_error")}</div>`;
    return;
  }

  const data = await response.json();
  if (!data.length) {
    historyBox.innerHTML = `<div class="history-empty">${t("history_empty")}</div>`;
    return;
  }

  historyBox.innerHTML = data
    .map(
      (item) => `
        <div class="history-item">
          <strong>${item.crop || t("history_crop_unknown")}</strong>
          <span>${item.yield ?? "N/A"} ton/hectare</span>
          <span>${item.risk || t("history_risk_unknown")}</span>
        </div>
      `
    )
      .join("");
}

function setAdvisoryFeedback(message, tone = "info", target = advisorySettingsFeedback) {
  if (!target) {
    return;
  }
  target.textContent = message;
  target.className = `dataset-status is-${tone}`;
}

function updateAdvisoryBadge(count) {
  if (!advisoryBadge) {
    return;
  }
  advisoryBadge.textContent = String(count || 0);
  advisoryBadge.classList.toggle("is-hidden", !count);
}

function updateAdvisoryStatus() {
  if (advisoryStatusPill) {
    advisoryStatusPill.textContent = "Auto monitoring active";
  }
}

function renderAdvisoryPopover(notifications = []) {
  if (!advisoryPopoverList) {
    return;
  }
  const visibleItems = notifications.slice(0, 2);
  if (!visibleItems.length) {
    advisoryPopoverList.innerHTML = `<div class="history-empty">${t("advisory_empty")}</div>`;
    if (advisoryPopoverMore) {
      advisoryPopoverMore.classList.add("is-hidden");
    }
    return;
  }
  advisoryPopoverList.innerHTML = visibleItems
    .map(
      (item) => `
        <article class="advisory-popover-item ${item.is_read ? "" : "is-unread"}">
          <span class="notification-priority ${item.priority || "info"}">${item.priority_label || item.priority || "Opportunity"}</span>
          <strong>${item.icon || "🚀"} ${item.compact_title || item.title}</strong>
          <p>👉 ${item.compact_action || item.recommendation || item.message || ""}</p>
        </article>
      `
    )
    .join("");
  if (advisoryPopoverMore) {
    advisoryPopoverMore.classList.toggle("is-hidden", notifications.length <= 2);
  }
}

function getFilteredNotifications(notifications = []) {
  return notifications.filter((item) => {
    const categoryMatch = advisoryFilters.category === "all" || (item.ui_tone || item.category) === advisoryFilters.category;
    const severityMatch = advisoryFilters.severity === "all" || item.priority === advisoryFilters.severity;
    return categoryMatch && severityMatch;
  });
}

function renderNotifications(notifications = []) {
  if (!notificationList) {
    return;
  }
  const filtered = getFilteredNotifications(notifications);
  if (!filtered.length) {
    notificationList.innerHTML = `<div class="history-empty">${t("advisory_empty")}</div>`;
    return;
  }

  const visibleAlerts = alertsExpanded ? filtered : filtered.slice(0, 3);
  notificationList.innerHTML = visibleAlerts
    .map(
      (item) => `
        <article class="notification-card compact ${item.ui_tone || item.category || "general"} ${item.priority || "info"} ${item.is_read ? "" : "is-unread"}" data-notification-id="${item.id}">
          <div class="notification-card-head">
            <span class="notification-priority ${item.priority || "info"}">${item.priority_label || item.priority || "Opportunity"}</span>
            <span class="notification-time">${item.region_label || item.created_at || ""}</span>
          </div>
          <h4>${item.icon || "🚀"} ${item.compact_title || item.title}</h4>
          <p>👉 ${item.compact_action || item.recommendation || item.message}</p>
          <div class="notification-actions">
            ${item.cta_href ? `<a href="${item.cta_href}" class="button secondary">Compare Crops</a>` : ""}
            <button type="button" class="button secondary notification-expand-button" data-notification-expand="${item.id}" aria-expanded="false">View details</button>
            ${item.is_read ? "" : `<button type="button" class="button secondary notification-read-button" data-notification-read="${item.id}">Mark done</button>`}
          </div>
          <div class="notification-detail is-hidden" data-notification-detail="${item.id}">
            <p><strong>Reason:</strong> ${item.message}</p>
            ${(item.best_yield !== null && item.best_yield !== undefined) || (item.watch_risk !== null && item.watch_risk !== undefined) || item.confidence_score ? `<div class="notification-meta-grid">${item.best_yield !== null && item.best_yield !== undefined ? `<span>📈 Yield: ${item.best_yield} t/ha</span>` : ""}${item.watch_risk !== null && item.watch_risk !== undefined ? `<span>⚠️ Risk: ${item.watch_risk}</span>` : ""}${item.confidence_score ? `<span>📊 Confidence: ${item.confidence_bar || buildConfidenceBar(item.confidence_score)}</span>` : ""}${item.expected_impact_pct !== null && item.expected_impact_pct !== undefined ? `<span>📈 Impact: ${item.expected_impact_pct}%</span>` : ""}</div>` : ""}
            ${item.recommendation ? `<div class="notification-recommendation"><strong>Recommendation:</strong> ${item.recommendation}</div>` : ""}
            ${item.action_items?.length ? `<ul class="notification-action-list">${item.action_items.map((action) => `<li>${action}</li>`).join("")}</ul>` : ""}
          </div>
        </article>
      `
    )
    .join("");

  if (toggleAlertsViewButton) {
    toggleAlertsViewButton.classList.toggle("is-hidden", filtered.length <= 3);
    toggleAlertsViewButton.textContent = alertsExpanded ? "Show Less" : "Show More";
  }
}

function buildConfidenceBar(score = 0) {
  const filled = Math.max(1, Math.min(10, Math.round(Number(score || 0) / 10)));
  return `${"█".repeat(filled)}${"░".repeat(Math.max(0, 10 - filled))} ${Math.round(Number(score || 0))}%`;
}

function renderDecisionHero(notifications = []) {
  if (!decisionHeroTitle || !decisionHeroSubtitle || !decisionHeroMetrics) {
    return;
  }
  const fallback = advisoryState.main_recommendation || {};
  const preferred = notifications.find((item) => item.category === "opportunity")
    || notifications.find((item) => (item.ui_tone || item.category) === "risk")
    || notifications.find((item) => item.priority === "critical")
    || notifications[0];
  if (!preferred) {
    decisionHeroTitle.textContent = `🚀 ${fallback.title || "Best Action Today"}`;
    decisionHeroSubtitle.textContent = `👉 ${fallback.reason || "No major alert right now. Current recommendation remains active."}`;
    decisionHeroMetrics.innerHTML = `
      <span>📍 ${fallback.region || "Target region"}</span>
      <span>${fallback.yield !== null && fallback.yield !== undefined ? `📈 Yield ${fallback.yield} t/ha` : "📈 Yield check ready"}</span>
      <span>⚠️ ${(fallback.risk_level || "Low")} risk</span>
      <span>📊 ${fallback.confidence_bar || buildConfidenceBar(fallback.confidence_score || 0)}</span>
    `;
    return;
  }

  const tone = preferred.ui_tone || preferred.category;
  const cropName = preferred.best_crop || preferred.watch_crop || preferred.compact_title || preferred.title || "this crop";
  const actionLabel = tone === "risk"
    ? "Protect"
    : tone === "watch"
    ? "Watch"
    : "Grow";

  decisionHeroTitle.textContent = `${preferred.icon || "🚀"} ${actionLabel} ${cropName}`;
  decisionHeroSubtitle.textContent = `👉 ${preferred.compact_action || preferred.recommendation || preferred.message}`;
  decisionHeroMetrics.innerHTML = `
    <span>📍 ${preferred.region_name || preferred.region_label || "Target region"}</span>
    <span>${preferred.best_yield !== null && preferred.best_yield !== undefined ? `📈 Yield ${preferred.best_yield} t/ha` : "📈 Yield check ready"}</span>
    <span>${preferred.best_risk_level || preferred.watch_risk_level ? `⚠️ ${preferred.best_risk_level || preferred.watch_risk_level} risk` : "⚠️ Risk check ready"}</span>
    <span>📊 ${preferred.confidence_bar || buildConfidenceBar(preferred.confidence_score || 0)}</span>
  `;
}

function renderQuickInsights(notifications = [], engineState = {}) {
  if (!advisoryQuickInsights) {
    return;
  }
  const topOpportunity = notifications.find((item) => (item.ui_tone || item.category) === "opportunity");
  const topRisk = notifications.find((item) => (item.ui_tone || item.category) === "risk");
  const topWatch = notifications.find((item) => (item.ui_tone || item.category) === "watch");
  const cards = [
    {
      label: "🌾 Best crop",
      value: engineState?.last_best_crop || topOpportunity?.best_crop || "Waiting",
      tone: "opportunity",
    },
    {
      label: "⚠️ Risk",
      value: topRisk ? "Action needed" : "Stable",
      tone: topRisk ? "risk" : "watch",
    },
    {
      label: "📈 Trend",
      value: engineState?.last_change_type || "steady",
      tone: "summary",
    },
  ];
  advisoryQuickInsights.innerHTML = cards
    .map(
      (card) => `
        <article class="quick-insight-card ${card.tone}">
          <span>${card.label}</span>
          <strong>${card.value}</strong>
        </article>
      `
    )
    .join("");
}

function syncAdvisoryState(nextState = {}) {
  advisoryState = {
    ...advisoryState,
    ...nextState,
  };
  renderNotifications(advisoryState.notifications || []);
  renderAdvisoryPopover(advisoryState.notifications || []);
  renderDecisionHero(advisoryState.notifications || []);
  renderQuickInsights(advisoryState.notifications || [], advisoryState.engine_state || {});
  updateAdvisoryBadge(advisoryState.unread_count || 0);
  updateAdvisoryStatus();
}

function showAdvisorySpotlight(popup) {
  if (!advisorySpotlight || !popup) {
    return;
  }
  advisorySpotlight.dataset.popupId = popup.id || "";
  if (advisorySpotlightPriority) {
    advisorySpotlightPriority.className = `notification-priority ${popup.priority || "important"}`;
    advisorySpotlightPriority.textContent = popup.priority || "Important";
  }
  if (advisorySpotlightTitle) {
    advisorySpotlightTitle.textContent = `${popup.icon || "🚀"} ${popup.compact_title || popup.title || "Priority advisory"}`;
  }
  if (advisorySpotlightMessage) {
    advisorySpotlightMessage.textContent = `👉 ${popup.compact_action || popup.recommendation || popup.message || ""}`;
  }
  if (advisorySpotlightRecommendation && advisorySpotlightNote) {
    const recommendation = popup.recommendation || "";
    advisorySpotlightRecommendation.textContent = recommendation;
    advisorySpotlightNote.classList.toggle("is-hidden", !recommendation);
  }
  if (advisorySpotlightLink) {
    advisorySpotlightLink.href = popup.cta_href || "#advisory-center";
  }
  advisorySpotlight.classList.add("is-open");
}

function hideAdvisorySpotlight() {
  advisorySpotlight?.classList.remove("is-open");
}

async function refreshAdvisories() {
  if (refreshAdvisoriesButton) {
    refreshAdvisoriesButton.disabled = true;
  }
  setAdvisoryFeedback(t("advisory_refreshing"), "pending");

  try {
    const response = await fetch("/api/notifications/refresh", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    });
    const data = await response.json();
    if (!response.ok) {
      setAdvisoryFeedback(data.error || t("compare_error"), "error");
      return;
    }
    syncAdvisoryState(data);
    setAdvisoryFeedback(t("advisory_refresh_done"), "success");
  } catch (error) {
    setAdvisoryFeedback(t("advisory_refresh_failed"), "error");
  } finally {
    if (refreshAdvisoriesButton) {
      refreshAdvisoriesButton.disabled = false;
    }
  }
}

async function saveAdvisoryPreferences(event) {
  event.preventDefault();
  if (!advisoryPreferencesForm) {
    return;
  }

  const formData = new FormData(advisoryPreferencesForm);
  const payload = {
    favorite_crop: formData.get("favorite_crop"),
    preferred_region: formData.get("preferred_region"),
    preferred_season: formData.get("preferred_season"),
    alert_frequency: formData.get("alert_frequency"),
    in_app_alerts_enabled: formData.get("in_app_alerts_enabled") ? "1" : "0",
  };

  setAdvisoryFeedback(t("advisory_saving"), "pending");
  try {
    const response = await fetch("/api/advisory-preferences", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await response.json();
    if (!response.ok) {
      setAdvisoryFeedback(data.error || t("advisory_save_error"), "error");
      return;
    }
    syncAdvisoryState(data);
    setAdvisoryFeedback(t("advisory_saved"), "success");
  } catch (error) {
    setAdvisoryFeedback(t("advisory_save_error"), "error");
  }
}

async function markAdvisoryRead(notificationId) {
  const response = await fetch(`/api/notifications/${notificationId}/read`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ dismiss_popup: true }),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    return;
  }
  const nextNotifications = (advisoryState.notifications || []).map((item) =>
    Number(item.id) === Number(notificationId) ? { ...item, is_read: true } : item
  );
  syncAdvisoryState({
    notifications: nextNotifications,
    unread_count: data.unread_count ?? advisoryState.unread_count,
  });
  if (advisorySpotlight?.dataset?.popupId === String(notificationId)) {
    hideAdvisorySpotlight();
  }
}

async function dismissAdvisoryPopup() {
  const popupId = advisorySpotlight?.dataset?.popupId;
  if (!popupId) {
    hideAdvisorySpotlight();
    return;
  }
  await fetch(`/api/notifications/${popupId}/dismiss`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  }).catch(() => null);
  hideAdvisorySpotlight();
  setAdvisoryFeedback(t("advisory_popup_dismissed"), "info");
}

async function applyDatasetRow() {
  const rowId = datasetRowSelect.value;
  if (!rowId) {
    resultBox.innerHTML = `<div class="result-placeholder">${t("result_placeholder")}</div>`;
    return;
  }

  const response = await fetch(`/api/dataset-row/${rowId}`);
  const result = await response.json();
  if (!response.ok) {
    showError(result.error || t("load_dataset_error"));
    return;
  }

  populateForm(result);
  datasetRowSelect.value = rowId;
  resultBox.innerHTML = `
    <div class="result-summary">
      <h2>${t("dataset_row_loaded")}</h2>
      <p><b>${t("crop")}:</b> ${result.crop_type}</p>
      <p><b>${t("region")}:</b> ${result.region}</p>
      <p><b>${t("original_yield")}:</b> ${result.yield_ton_per_hectare} ton/hectare</p>
      <p>${t("dataset_loaded_note")}</p>
    </div>
  `;
  updateContextCharts(getFormData());
  updateChartContext(getFormData());
}

function applyAssistantSelections() {
  if (simpleRain) simpleRain.value = assistantRain.value;
  if (simplePest) simplePest.value = assistantPest.value;
  if (simpleFertilizer) simpleFertilizer.value = assistantFertilizer.value;
  if (assistantSoil?.value) {
    const soilField = form.elements.namedItem("soil_type");
    if (soilField) {
      soilField.value = assistantSoil.value;
    }
  }
  renderSimplePreview();
  resultBox.innerHTML = `<div class="result-placeholder reveal-in">${t("assistant_applied")}</div>`;
  if (assistantChatbot) {
    assistantChatbot.classList.remove("is-open");
  }
}

function loadExampleProfile() {
  if (!form) {
    return;
  }
  populateForm(window.cropAppDefaults);
  renderSimplePreview();
  resultBox.innerHTML = `<div class="result-placeholder reveal-in">${t("dataset_loaded_note")}</div>`;
  assistantChatbot?.classList.remove("is-open");
}

function openWorkflowReview() {
  document.getElementById("workflow-review")?.scrollIntoView({ behavior: "smooth", block: "start" });
  assistantChatbot?.classList.remove("is-open");
}

function openAdvisoryCenterPage() {
  window.location.href = "/advisory";
}

function openHistoryPanel() {
  document.getElementById("history")?.scrollIntoView({ behavior: "smooth", block: "start" });
  assistantChatbot?.classList.remove("is-open");
}

function openDatasetControls() {
  document.querySelector(".dataset-manager-panel")?.scrollIntoView({ behavior: "smooth", block: "start" });
  assistantChatbot?.classList.remove("is-open");
}

function openContactPanel() {
  document.getElementById("contact")?.scrollIntoView({ behavior: "smooth", block: "start" });
  assistantChatbot?.classList.remove("is-open");
}

function setModelGuideTab(tabName = "overview") {
  modelGuideTabs.forEach((button) => {
    button.classList.toggle("is-active", button.dataset.guideTab === tabName);
  });
  modelGuidePanels.forEach((panel) => {
    panel.classList.toggle("is-active", panel.dataset.guidePanel === tabName);
  });
}

function setWorkflowGuideStep(stepKey = "input") {
  const detail = workflowGuideDetails[stepKey];
  if (!detail) {
    return;
  }
  workflowStepButtons.forEach((button) => {
    button.classList.toggle("is-active", button.dataset.workflowStep === stepKey);
  });
  if (workflowDetailPill) workflowDetailPill.textContent = detail.pill;
  if (workflowDetailTitle) workflowDetailTitle.textContent = detail.title;
  if (workflowDetailCopy) workflowDetailCopy.textContent = detail.copy;
  if (workflowDetailWhy) workflowDetailWhy.textContent = detail.why;
  if (workflowDetailNext) workflowDetailNext.textContent = detail.next;
}

function toggleModelGuideDrawer(forceOpen = null) {
  if (!modelGuideDrawer) {
    return;
  }
  const shouldOpen = forceOpen === null
    ? !modelGuideDrawer.classList.contains("is-open")
    : Boolean(forceOpen);
  modelGuideDrawer.classList.toggle("is-open", shouldOpen);
  modelGuideDrawer.setAttribute("aria-hidden", shouldOpen ? "false" : "true");
  document.body.classList.toggle("model-guide-open", shouldOpen);
}

function resetPredictionExperience() {
  showValidationErrors([]);
  populateForm(window.cropAppDefaults);
  if (datasetRowSelect) datasetRowSelect.value = "";
  if (simpleCountry) simpleCountry.selectedIndex = 0;
  if (simpleDirection) simpleDirection.value = "South";
  if (simpleSeason) simpleSeason.value = "Kharif";
  if (simpleCrop) simpleCrop.value = window.cropAppDefaults.crop_type;
  if (simpleRain) simpleRain.value = "normal";
  if (simpleFertilizer) simpleFertilizer.value = "medium";
  if (simplePest) simplePest.value = "low";
  renderSimplePreview();
  resultBox.innerHTML = `<div class="result-placeholder">${t("result_placeholder")}</div>`;
  if (compareCurrent) compareCurrent.innerHTML = t("compare_placeholder_current");
  if (compareImproved) compareImproved.innerHTML = t("compare_placeholder_improved");
  if (chartContextBanner) {
    chartContextBanner.textContent = t("chart_context_empty");
  }
  lastPrediction = null;
  lastScenarioInput = null;
  predictionExplainerOpen = false;
}

function togglePredictionExplainer() {
  predictionExplainerOpen = !predictionExplainerOpen;
  const panel = document.getElementById("prediction-stage-list");
  if (panel) {
    panel.classList.toggle("is-open", predictionExplainerOpen);
  }
}

window.__togglePredictionExplainer = togglePredictionExplainer;

async function compareScenarios() {
  if (!lastScenarioInput) {
    showError(t("compare_requires_prediction"));
    return;
  }

  const response = await fetch("/api/compare-scenarios", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      current_input: lastScenarioInput,
      improved_input: null,
    }),
  });

  if (!response.ok) {
    showError(t("compare_error"));
    return;
  }

  const result = await response.json();
  renderCompareCards(result);
}

async function downloadReport() {
  if (!lastPrediction) {
    showError("Run a prediction first so a report can be created.");
    return;
  }

  const reportData = {
    crop_type: lastPrediction.payload.crop_type,
    region: lastPrediction.payload.region,
    predicted_yield: lastPrediction.result.predicted_yield,
    risk_level: lastPrediction.result.risk.level,
    confidence_score: lastPrediction.result.confidence.score,
    confidence_label: lastPrediction.result.confidence.label,
    recommended_crop: lastPrediction.result.recommended_crop,
    action_cards: lastPrediction.result.action_cards,
    insights: lastPrediction.result.insights,
    recommendations: lastPrediction.result.recommendations,
  };

  const response = await fetch("/api/export-report", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ report_data: reportData }),
  });

  if (!response.ok) {
    showError("Report export failed.");
    return;
  }

  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "crop_yield_report.html";
  link.click();
  URL.revokeObjectURL(url);
}

function saveTrendChartImage() {
  if (typeof Plotly === "undefined") {
    return;
  }
  const chart = trendChartContainer?.querySelector(".dynamic-chart, .plotly-graph-div");
  if (!chart) {
    showError("Generate the trend chart first before saving an image.");
    return;
  }
  Plotly.downloadImage(chart, { format: "png", filename: "rainfall_yield_trend" });
}

function setDatasetStatus(message, tone = "info") {
  if (!datasetStatus) {
    return;
  }
  datasetStatus.textContent = message;
  datasetStatus.className = `dataset-status is-${tone}`;
}

async function uploadDataset(event) {
  event.preventDefault();

  if (!datasetFileInput?.files?.length) {
    setDatasetStatus("Choose a CSV file first.", "error");
    return;
  }

  const formData = new FormData();
  formData.append("dataset_file", datasetFileInput.files[0]);
  setDatasetStatus("Uploading dataset and retraining models...", "pending");

  try {
    const response = await fetch("/api/upload-dataset", {
      method: "POST",
      body: formData,
    });
    const data = await response.json();

    if (!response.ok) {
      setDatasetStatus(data.error || "Dataset upload failed.", "error");
      return;
    }

    setDatasetStatus(
      `${data.message} Active dataset: ${data.dataset_name}. Best model: ${data.best_model}. Refreshing...`,
      "success"
    );
    window.setTimeout(() => window.location.reload(), 1200);
  } catch (error) {
    setDatasetStatus("Dataset upload failed due to a network or server issue.", "error");
  }
}

async function resetDataset() {
  setDatasetStatus("Resetting back to the built-in demo dataset...", "pending");

  try {
    const response = await fetch("/api/reset-dataset", {
      method: "POST",
    });
    const data = await response.json();

    if (!response.ok) {
      setDatasetStatus(data.error || "Dataset reset failed.", "error");
      return;
    }

    setDatasetStatus(
      `${data.message} Active dataset: ${data.dataset_name}. Best model: ${data.best_model}. Refreshing...`,
      "success"
    );
    window.setTimeout(() => window.location.reload(), 1200);
  } catch (error) {
    setDatasetStatus("Dataset reset failed due to a network or server issue.", "error");
  }
}

if (languageSelect) {
  languageSelect.addEventListener("change", (event) => {
    applyLanguage(event.target.value);
  });
}

if (themeSelect) {
  themeSelect.addEventListener("change", (event) => {
    applyTheme(event.target.value);
  });
}

if (applyAssistantButton) {
  applyAssistantButton.addEventListener("click", applyAssistantSelections);
}

if (assistantFab) {
  assistantFab.addEventListener("click", () => {
    assistantChatbot?.classList.toggle("is-open");
  });
}

if (advisoryToggle) {
  advisoryToggle.addEventListener("click", () => {
    advisoryPopover?.classList.toggle("is-open");
  });
}

if (modelGuideToggle) {
  modelGuideToggle.addEventListener("click", () => {
    toggleModelGuideDrawer(true);
  });
}

modelGuideTabs.forEach((button) => {
  button.addEventListener("click", () => {
    setModelGuideTab(button.dataset.guideTab || "overview");
  });
});

workflowStepButtons.forEach((button) => {
  button.addEventListener("click", () => {
    setWorkflowGuideStep(button.dataset.workflowStep || "input");
  });
});

if (assistantClose) {
  assistantClose.addEventListener("click", () => {
    assistantChatbot?.classList.remove("is-open");
  });
}

if (quickOpenAdvisoryButton) {
  quickOpenAdvisoryButton.addEventListener("click", openAdvisoryCenterPage);
}

if (quickOpenHistoryButton) {
  quickOpenHistoryButton.addEventListener("click", openHistoryPanel);
}

if (quickOpenDatasetButton) {
  quickOpenDatasetButton.addEventListener("click", openDatasetControls);
}

if (quickOpenContactButton) {
  quickOpenContactButton.addEventListener("click", openContactPanel);
}

if (modelGuideClose) {
  modelGuideClose.addEventListener("click", () => {
    toggleModelGuideDrawer(false);
  });
}

if (userMenuToggle) {
  userMenuToggle.addEventListener("click", () => {
    userMenuDropdown?.classList.toggle("is-open");
  });
}

document.addEventListener("click", (event) => {
  if (userMenuDropdown && userMenuToggle) {
    if (!userMenuDropdown.contains(event.target) && !userMenuToggle.contains(event.target)) {
      userMenuDropdown.classList.remove("is-open");
    }
  }

  if (advisoryPopover && advisoryToggle) {
    if (!advisoryPopover.contains(event.target) && !advisoryToggle.contains(event.target)) {
      advisoryPopover.classList.remove("is-open");
    }
  }

  if (modelGuideDrawer?.classList.contains("is-open")) {
    const drawerPanel = modelGuideDrawer.querySelector(".model-guide-drawer-panel");
    if (drawerPanel && !drawerPanel.contains(event.target) && !modelGuideToggle?.contains(event.target)) {
      toggleModelGuideDrawer(false);
    }
  }

  const readButton = event.target.closest("[data-notification-read]");
  if (readButton) {
    markAdvisoryRead(readButton.dataset.notificationRead);
  }

  const expandButton = event.target.closest("[data-notification-expand]");
  if (expandButton) {
    const targetId = expandButton.dataset.notificationExpand;
    const detailBox = document.querySelector(`[data-notification-detail="${targetId}"]`);
    if (detailBox) {
      detailBox.classList.toggle("is-hidden");
      expandButton.setAttribute("aria-expanded", detailBox.classList.contains("is-hidden") ? "false" : "true");
      expandButton.textContent = detailBox.classList.contains("is-hidden") ? "View details" : "Hide details";
    }
  }
});

if (resetFormButton) {
  resetFormButton.addEventListener("click", resetPredictionExperience);
}

if (compareScenariosButton) {
  compareScenariosButton.addEventListener("click", compareScenarios);
}

if (downloadReportButton) {
  downloadReportButton.addEventListener("click", downloadReport);
}

if (saveChartImageButton) {
  saveChartImageButton.addEventListener("click", saveTrendChartImage);
}

if (datasetUploadForm) {
  datasetUploadForm.addEventListener("submit", uploadDataset);
}

if (datasetResetButton) {
  datasetResetButton.addEventListener("click", resetDataset);
}

if (refreshAdvisoriesButton) {
  refreshAdvisoriesButton.addEventListener("click", refreshAdvisories);
}

if (toggleAlertsViewButton) {
  toggleAlertsViewButton.addEventListener("click", () => {
    alertsExpanded = !alertsExpanded;
    renderNotifications(advisoryState.notifications || []);
  });
}

if (advisoryPreferencesForm) {
  advisoryPreferencesForm.addEventListener("submit", saveAdvisoryPreferences);
}

if (advisoryCategoryFilter) {
  advisoryCategoryFilter.addEventListener("change", (event) => {
    advisoryFilters.category = event.target.value;
    renderNotifications(advisoryState.notifications || []);
  });
}

if (advisorySeverityFilter) {
  advisorySeverityFilter.addEventListener("change", (event) => {
    advisoryFilters.severity = event.target.value;
    renderNotifications(advisoryState.notifications || []);
  });
}

if (advisorySpotlightClose) {
  advisorySpotlightClose.addEventListener("click", dismissAdvisoryPopup);
}

if (advisorySpotlightDismiss) {
  advisorySpotlightDismiss.addEventListener("click", dismissAdvisoryPopup);
}

if (decisionHeroDetails) {
  decisionHeroDetails.addEventListener("click", () => {
    document.getElementById("advisory-center")?.scrollIntoView({ behavior: "smooth", block: "start" });
  });
}

if (togglePredictionExplainerButton) {
  togglePredictionExplainerButton.addEventListener("click", togglePredictionExplainer);
}

if (form) {
  form.addEventListener("submit", submitPrediction);
}

if (fillExampleButton) {
  fillExampleButton.addEventListener("click", () => populateForm(window.cropAppDefaults));
}

if (recommendCropButton) {
  recommendCropButton.addEventListener("click", recommendCrop);
}

if (datasetRowSelect) {
  datasetRowSelect.addEventListener("change", applyDatasetRow);
}

modeButtons.forEach((button) => {
  button.addEventListener("click", () => setMode(button.dataset.mode));
});

[simpleCountry, simpleDirection, simpleSeason, simpleCrop, simpleRain, simpleFertilizer, simplePest].forEach((field) => {
  if (field) {
    field.addEventListener("change", renderSimplePreview);
  }
});

infoToggleButtons.forEach((button) => {
  button.addEventListener("click", () => {
    openInfoModal(button.dataset.infoKey);
  });
});

if (infoModalClose) {
  infoModalClose.addEventListener("click", closeInfoModal);
}

if (infoModal) {
  infoModal.addEventListener("click", (event) => {
    if (event.target === infoModal) {
      closeInfoModal();
    }
  });
}

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    closeInfoModal();
    toggleModelGuideDrawer(false);
  }
});

applyTheme(currentTheme);
applyLanguage(currentLanguage);
loadHistory();
syncAdvisoryState(advisoryState);
setModelGuideTab("overview");
setWorkflowGuideStep("input");
showAdvisorySpotlight(advisoryState.priority_popup);
window.addEventListener("load", () => {
  setTimeout(syncPlotlyTheme, 120);
});
